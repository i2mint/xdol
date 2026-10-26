"""Persisted, mergeable per-key access statistics for any store.

An :class:`AccessLog` is an append-only JSONL file of access events, folded into an
in-memory ``{key: AccessStats}`` on open; the log itself is a read-only ``Mapping`` of
key to (immutable) :class:`AccessStats`. :func:`track_access` wraps any
``MutableMapping`` so reads, writes and deletes are recorded. The log is the
``tracker`` of :class:`xdol.tiering.TieredStore`.

Filesystem atime cannot do this job (under ``relatime`` it is never a count, and any
backup pass resets it), hence an explicit log.

Event schema, one JSON object per line (plain string keys, so a filesystem tracer that
only knows paths can append to the same file)::

    {"k": key, "op": "read"|"write"|"delete"|"release"|"pin"|"unpin",
     "n": bytes, "from": "hot"|"cold"|null, "ts": unix_seconds, "tag": str|null}

Only ``k`` and ``op`` are required: an event without ``n`` leaves the size alone.
Replay does not trust file order (several writers may append to one file): recency
fields take the max timestamp seen, ``first_seen`` the min.

Keys must be JSON scalars, in practice ``str``: a key that does not survive a JSON
round trip (a tuple, a ``Path``...) is counted in memory but lost or changed on reopen,
and recording one warns once.

A compacted file holds ``{"k": key, "op": "snapshot", "stats": {...}}`` lines, folded as
initial state.

>>> log = AccessLog()
>>> store = track_access({'a': b'xyz'}, tracker=log)
>>> store['a']
b'xyz'
>>> log['a'].reads
1
>>> 'a' in store  # membership is never recorded
True
>>> log['a'].frequency
1
"""

from __future__ import annotations

import json
import os
import platform
import time
import warnings
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from dataclasses import asdict, dataclass, fields, replace
from typing import Any

from xdol.util import atomic_write

OPS = ("read", "write", "delete", "release", "pin", "unpin")
_CREATING_OPS = ("read", "write", "pin")  # other ops on an unknown key are no-ops


@dataclass(frozen=True)
class AccessStats:
    """Access statistics of one key (immutable: update with ``dataclasses.replace``)."""

    size: int = 0
    reads: int = 0
    writes: int = 0
    first_seen: float = 0.0
    last_access: float = 0.0
    last_read: float = 0.0
    pinned: bool = False

    @property
    def frequency(self) -> int:
        return self.reads + self.writes


_STATS_FIELDS = {f.name for f in fields(AccessStats)}


def value_size(v) -> int:
    """``len(v)``, or ``0`` when ``v`` has no length (tracking never raises)."""
    try:
        return len(v)
    except TypeError:
        return 0


def _min_nonzero(a: float, b: float) -> float:
    """The smaller of two timestamps, ``0.0`` meaning unknown."""
    return min((t for t in (a, b) if t), default=0.0)


def _survives_json(key) -> bool:
    if type(key) is str:
        return True
    try:
        back = json.loads(json.dumps(key))
    except (TypeError, ValueError):
        return False
    return type(back) is type(key) and back == key


def _is_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


_FIELD_CHECKS = {  # AccessStats field -> type check (a foreign line must not poison replay)
    "size": lambda x: isinstance(x, int) and not isinstance(x, bool),
    "reads": lambda x: isinstance(x, int) and not isinstance(x, bool),
    "writes": lambda x: isinstance(x, int) and not isinstance(x, bool),
    "first_seen": _is_number,
    "last_access": _is_number,
    "last_read": _is_number,
    "pinned": lambda x: isinstance(x, bool),
}
assert set(_FIELD_CHECKS) == _STATS_FIELDS


def _stats_from_snapshot(data: dict) -> AccessStats:
    """``AccessStats`` from a snapshot's dict; unknown fields dropped, bad types rejected."""
    data = {k: v for k, v in data.items() if k in _STATS_FIELDS}
    if not all(_FIELD_CHECKS[k](v) for k, v in data.items()):
        raise TypeError("snapshot field with the wrong type")
    return AccessStats(**data)


def _apply(stats_by_key: dict, key, op, nbytes, ts) -> None:
    """Fold one event into ``stats_by_key`` (shared by live recording and replay).

    ``nbytes=None`` leaves the size alone. Order-independent: recency is a max,
    ``first_seen`` a min, and the size comes from the latest read or write.
    """
    if op not in OPS:
        return
    if op == "delete":
        stats_by_key.pop(key, None)
        return
    st = stats_by_key.get(key)
    if st is None:
        if op not in _CREATING_OPS:
            return
        st = AccessStats(first_seen=ts)
    if op in ("read", "write"):
        changes = dict(
            first_seen=_min_nonzero(st.first_seen, ts),
            last_access=max(st.last_access, ts),
        )
        if nbytes is not None and ts >= st.last_access:
            changes["size"] = nbytes
        if op == "read":
            changes.update(reads=st.reads + 1, last_read=max(st.last_read, ts))
        else:
            changes.update(writes=st.writes + 1)
        st = replace(st, **changes)
    elif op in ("pin", "unpin"):
        st = replace(st, pinned=op == "pin")
    # "release": the hot copy went away; frequency history is kept deliberately.
    stats_by_key[key] = st


class AccessLog(Mapping):
    """Append-only JSONL access log, readable as a ``Mapping`` of key -> AccessStats.

    :param path: log file; ``None`` keeps everything in memory (tests).
    :param clock: callable returning unix seconds.
    :param tag: label stamped on each event; defaults to ``platform.node()``.

    A tracker never raises into a store's read path: if the file cannot be written,
    a warning is issued once and counting continues in memory. Not thread-safe.
    """

    # a log is a resource, not a value: keep identity semantics (Mapping drops them)
    __eq__ = object.__eq__
    __hash__ = object.__hash__

    def __init__(
        self,
        path: str | os.PathLike | None = None,
        *,
        clock: Callable[[], float] = time.time,
        tag: str | None = None,
    ):
        self.path = None if path is None else os.fspath(path)
        self.clock = clock
        self.tag = platform.node() if tag is None else tag
        self._stats: dict[Any, AccessStats] = {}
        self._fh = None
        self._warned_io = False
        self._warned_key = False
        self._meta: dict[str, Any] = {}  # small JSON values (e.g. a policy's state)
        self._meta_ts: dict[str, float] = {}
        if self.path is not None and os.path.exists(self.path):
            self._fold_file()

    # -- reading -----------------------------------------------------------
    def _fold_file(self) -> None:
        """Replay the file. Recency is order-independent; so are delete/pin/unpin, which
        are ordered by their timestamps (a write-ahead merge of several writers' lines)."""
        state_ts: dict = {}  # key -> ts of the last applied delete/pin/unpin
        deleted_at: dict = {}  # key -> ts of its delete: earlier reads/writes are void
        with open(self.path, "rb") as fh:
            for raw in fh:
                try:
                    ev = json.loads(raw.decode("utf-8"))
                    key, op = ev["k"], ev["op"]
                    ts, n = ev.get("ts") or 0.0, ev.get("n")
                    if not _is_number(ts) or not (n is None or _is_number(n)):
                        raise TypeError("timestamp or size of the wrong type")
                    if op == "snapshot":
                        self._stats[key] = _stats_from_snapshot(ev["stats"])
                    elif op == "meta":
                        if ts >= self._meta_ts.get(key, 0.0):
                            self._meta[key], self._meta_ts[key] = ev["value"], ts
                    elif op in ("delete", "pin", "unpin"):
                        if ts < state_ts.get(key, 0.0):
                            continue  # superseded by a later-stamped line seen earlier
                        state_ts[key] = ts
                        st = self._stats.get(key)
                        if op == "delete":
                            deleted_at[key] = max(ts, deleted_at.get(key, 0.0))
                            if st is not None and st.last_access > ts:
                                continue  # a later access is already folded in: keep it
                        _apply(self._stats, key, op, n, ts)
                    elif ts > deleted_at.get(key, -1.0):
                        _apply(self._stats, key, op, n, ts)
                except (ValueError, KeyError, TypeError, AttributeError):
                    continue  # torn, undecodable or foreign line: skip, never fail

    def __getitem__(self, key) -> AccessStats:
        return self._stats[key]

    def __contains__(self, key) -> bool:
        return key in self._stats

    def __iter__(self) -> Iterator:
        return iter(self._stats)

    def __len__(self) -> int:
        return len(self._stats)

    def stats(self, key) -> AccessStats | None:
        """Alias of ``self.get(key)``."""
        return self._stats.get(key)

    def get_meta(self, name: str, default=None):
        """A small JSON value stored with the log by :meth:`set_meta`."""
        return self._meta.get(name, default)

    # -- writing -----------------------------------------------------------
    def _warn_once(self, flag: str, message: str) -> None:
        if not getattr(self, flag):
            setattr(self, flag, True)
            warnings.warn(message)

    def _open_for_append(self):
        fh = open(self.path, "ab")
        if fh.tell():  # a torn last line (crash mid-write) must not eat our event
            with open(self.path, "rb") as r:
                r.seek(-1, os.SEEK_END)
                if r.read(1) != b"\n":
                    fh.write(b"\n")
        return fh

    def record(self, key, op: str, nbytes: int = 0, *, served_from=None) -> None:
        if op not in OPS:
            raise ValueError(f"Unknown op {op!r}; expected one of {OPS}")
        ts = self.clock()
        _apply(self._stats, key, op, nbytes, ts)
        if self.path is None:
            return
        if not _survives_json(key):
            self._warn_once(
                "_warned_key",
                f"AccessLog key {key!r} does not survive a JSON round trip: its "
                "stats will be lost or changed on reopen (use str keys).",
            )
        event = {
            "k": key,
            "op": op,
            "n": nbytes,
            "from": served_from,
            "ts": ts,
            "tag": self.tag,
        }
        self._append(event)

    def set_meta(self, name: str, value) -> None:
        """Store a small JSON value (last write wins by timestamp) alongside the stats.

        Used for state that should survive a restart but belongs to no key, e.g. a
        placement policy's aging clock. Like :meth:`record`, never raises on I/O errors.
        """
        ts = self.clock()
        self._meta[name], self._meta_ts[name] = value, ts
        if self.path is not None:
            self._append({"k": name, "op": "meta", "value": value, "ts": ts})

    def _append(self, event: dict) -> None:
        try:
            if self._fh is None:
                self._fh = self._open_for_append()
            self._fh.write((json.dumps(event, default=str) + "\n").encode("utf-8"))
            self._fh.flush()
        except OSError as e:
            self._warn_once(
                "_warned_io",
                f"AccessLog cannot write to {self.path!r} ({e}); "
                "counting in memory only.",
            )

    def compact(self) -> None:
        """Rewrite the file as one snapshot line per key, atomically.

        Safe only when no other writer has the file open: another process appending
        to it keeps writing to the replaced file, and those events are lost.
        """
        if self.path is None:
            return
        self.close()
        events = (
            *(
                {"k": key, "op": "snapshot", "stats": asdict(st)}
                for key, st in self._stats.items()
            ),
            *(
                {"k": name, "op": "meta", "value": v, "ts": self._meta_ts[name]}
                for name, v in self._meta.items()
            ),
        )
        atomic_write(
            self.path,
            (
                (json.dumps(event, default=str) + "\n").encode("utf-8")
                for event in events
            ),
        )

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def merge_stats(*logs: Mapping) -> dict:
    """Merge several ``Mapping[key, AccessStats]`` (e.g. logs) into one dict.

    Sums reads/writes, takes the ``size`` of the latest access, min ``first_seen``
    (ignoring unknown zeros), max ``last_access``/``last_read``, OR of ``pinned``.
    """
    out: dict[Any, AccessStats] = {}
    for log in logs:
        for key, st in log.items():
            cur = out.get(key)
            if cur is None:
                out[key] = st
                continue
            out[key] = replace(
                cur,
                reads=cur.reads + st.reads,
                writes=cur.writes + st.writes,
                size=st.size if st.last_access >= cur.last_access else cur.size,
                first_seen=_min_nonzero(cur.first_seen, st.first_seen),
                last_access=max(cur.last_access, st.last_access),
                last_read=max(cur.last_read, st.last_read),
                pinned=cur.pinned or st.pinned,
            )
    return out


def default_access_log_for(store, **kwargs) -> AccessLog:
    """An :class:`AccessLog` beside ``store``'s ``rootdir`` (never inside it).

    Without a ``rootdir``, an in-memory log, with a warning.
    """
    rootdir = getattr(store, "rootdir", None)
    if rootdir is None:
        warnings.warn(
            "store has no rootdir: access counts are kept in memory and will not "
            "survive the process."
        )
        return AccessLog(None, **kwargs)
    root = os.fspath(rootdir).rstrip("/\\" + os.sep)
    return AccessLog(root + ".access.jsonl", **kwargs)


class _TrackedStore(MutableMapping):
    """Plain MutableMapping wrapper recording accesses; see :func:`track_access`."""

    def __init__(self, store, tracker: AccessLog):
        self._store = store
        self._tracker = tracker

    @property
    def store(self):
        return self._store

    @property
    def tracker(self) -> AccessLog:
        return self._tracker

    def __getitem__(self, k):
        v = self._store[k]
        self._tracker.record(k, "read", value_size(v))
        return v

    def __setitem__(self, k, v):
        self._store[k] = v
        self._tracker.record(k, "write", value_size(v))

    def __delitem__(self, k):
        del self._store[k]
        self._tracker.record(k, "delete", 0)

    def __contains__(self, k):
        return k in self._store

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)


def track_access(
    store,
    *,
    tracker: AccessLog | None = None,
    clock: Callable[[], float] | None = None,
    tag: str | None = None,
) -> MutableMapping:
    """Wrap ``store`` so reads, writes and deletes are recorded in ``tracker``.

    ``__contains__``, ``__iter__`` and ``__len__`` are never recorded. Without a
    ``tracker``, :func:`default_access_log_for` picks one, built with ``clock`` and
    ``tag`` (passing either alongside a ``tracker`` is an error: set them on the
    log). A value without ``len`` is recorded with size 0. The wrapped store is
    available as ``.store`` and the log as ``.tracker``.
    """
    if tracker is None:
        clock = time.time if clock is None else clock
        tracker = default_access_log_for(store, clock=clock, tag=tag)
    elif clock is not None or tag is not None:
        raise ValueError("clock and tag configure the default tracker; set them on it")
    return _TrackedStore(store, tracker)
