"""Persisted, mergeable per-key access statistics for any store.

An :class:`AccessLog` is an append-only JSONL file of access events, folded into an
in-memory ``{key: AccessStats}`` on open. :func:`track_access` wraps any
``MutableMapping`` so reads, writes and deletes are recorded. The log is the
``tracker`` of :class:`xdol.tiering.TieredStore`.

Filesystem atime cannot do this job (under ``relatime`` it is never a count, and any
backup pass resets it), hence an explicit log.

Event schema, one JSON object per line (plain string keys, so a filesystem tracer that
only knows paths can append to the same file)::

    {"k": key, "op": "read"|"write"|"delete"|"release"|"pin"|"unpin",
     "n": bytes, "from": "hot"|"cold"|null, "ts": unix_seconds, "tag": str|null}

A compacted file holds ``{"k": key, "op": "snapshot", "stats": {...}}`` lines, folded as
initial state.

>>> log = AccessLog()
>>> store = track_access({'a': b'xyz'}, tracker=log)
>>> store['a']
b'xyz'
>>> log.stats('a').reads
1
>>> 'a' in store  # membership is never recorded
True
>>> log.stats('a').frequency
1
"""

from __future__ import annotations

import json
import os
import platform
import tempfile
import time
import warnings
from collections.abc import Callable, Iterator, MutableMapping
from dataclasses import asdict, dataclass, fields
from typing import Any

OPS = ("read", "write", "delete", "release", "pin", "unpin")


@dataclass
class AccessStats:
    """Access statistics of one key."""

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


def _apply(stats_by_key: dict, key, op, nbytes, ts) -> None:
    """Fold one event into ``stats_by_key`` (shared by live recording and replay)."""
    if op == "delete":
        stats_by_key.pop(key, None)
        return
    st = stats_by_key.get(key)
    if st is None:
        st = stats_by_key[key] = AccessStats(first_seen=ts)
    if op == "read":
        st.reads += 1
        st.size = nbytes
        st.last_access = st.last_read = ts
    elif op == "write":
        st.writes += 1
        st.size = nbytes
        st.last_access = ts
    elif op == "pin":
        st.pinned = True
    elif op == "unpin":
        st.pinned = False
    # "release": the hot copy went away; frequency history is kept deliberately.


class AccessLog:
    """Append-only JSONL access log with an in-memory aggregate.

    :param path: log file; ``None`` keeps everything in memory (tests).
    :param clock: callable returning unix seconds.
    :param tag: label stamped on each event; defaults to ``platform.node()``.

    A tracker never raises into a store's read path: if the file cannot be written,
    a warning is issued once and counting continues in memory.
    """

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
        self._warned = False
        if self.path is not None and os.path.exists(self.path):
            self._fold_file()

    # -- reading -----------------------------------------------------------
    def _fold_file(self) -> None:
        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    ev = json.loads(line)
                    key, op = ev["k"], ev["op"]
                    if op == "snapshot":
                        data = {
                            k: v for k, v in ev["stats"].items() if k in _STATS_FIELDS
                        }
                        self._stats[key] = AccessStats(**data)
                    else:
                        _apply(
                            self._stats, key, op, ev.get("n") or 0, ev.get("ts") or 0.0
                        )
                except (ValueError, KeyError, TypeError, AttributeError):
                    continue  # torn or foreign line: skip, never fail on open

    def stats(self, key) -> AccessStats | None:
        return self._stats.get(key)

    def __iter__(self) -> Iterator:
        return iter(self._stats)

    def __len__(self) -> int:
        return len(self._stats)

    def items(self):
        return self._stats.items()

    # -- writing -----------------------------------------------------------
    def record(self, key, op: str, nbytes: int = 0, *, served_from=None) -> None:
        if op not in OPS:
            raise ValueError(f"Unknown op {op!r}; expected one of {OPS}")
        ts = self.clock()
        _apply(self._stats, key, op, nbytes, ts)
        if self.path is None:
            return
        event = {
            "k": key,
            "op": op,
            "n": nbytes,
            "from": served_from,
            "ts": ts,
            "tag": self.tag,
        }
        try:
            if self._fh is None:
                self._fh = open(self.path, "a", encoding="utf-8")
            self._fh.write(json.dumps(event, default=str) + "\n")
            self._fh.flush()
        except OSError as e:
            if not self._warned:
                self._warned = True
                warnings.warn(
                    f"AccessLog cannot write to {self.path!r} ({e}); "
                    "counting in memory only."
                )

    def compact(self) -> None:
        """Rewrite the file as one snapshot line per key, atomically."""
        if self.path is None:
            return
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        directory = os.path.dirname(os.path.abspath(self.path))
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for key, st in self._stats.items():
                    line = {"k": key, "op": "snapshot", "stats": asdict(st)}
                    fh.write(json.dumps(line, default=str) + "\n")
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def merge_stats(*logs) -> dict:
    """Merge several logs (or ``{key: AccessStats}`` mappings) into one dict.

    Sums reads/writes, takes the latest ``size``, min ``first_seen``, max
    ``last_access``/``last_read``, OR of ``pinned``.
    """
    out: dict[Any, AccessStats] = {}
    latest: dict[Any, float] = {}
    for log in logs:
        items = log.items() if hasattr(log, "items") else log
        for key, st in items:
            cur = out.get(key)
            if cur is None:
                out[key] = AccessStats(**asdict(st))
                latest[key] = st.last_access
                continue
            cur.reads += st.reads
            cur.writes += st.writes
            if st.last_access >= latest[key]:
                cur.size = st.size
                latest[key] = st.last_access
            cur.first_seen = min(
                (t for t in (cur.first_seen, st.first_seen) if t), default=0.0
            )
            cur.last_access = max(cur.last_access, st.last_access)
            cur.last_read = max(cur.last_read, st.last_read)
            cur.pinned = cur.pinned or st.pinned
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
        self._tracker.record(k, "read", len(v))
        return v

    def __setitem__(self, k, v):
        self._store[k] = v
        self._tracker.record(k, "write", len(v))

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
    clock: Callable[[], float] = time.time,
    tag: str | None = None,
) -> MutableMapping:
    """Wrap ``store`` so reads, writes and deletes are recorded in ``tracker``.

    ``__contains__``, ``__iter__`` and ``__len__`` are never recorded. Without a
    ``tracker``, :func:`default_access_log_for` picks one. The wrapped store is
    available as ``.store`` and the log as ``.tracker``.
    """
    if tracker is None:
        tracker = default_access_log_for(store, clock=clock, tag=tag)
    return _TrackedStore(store, tracker)
