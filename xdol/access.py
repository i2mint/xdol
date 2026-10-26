"""Persisted, mergeable per-key access statistics for any store.

An :class:`AccessLog` is an append-only JSONL file of access events, folded into an
in-memory ``{key: AccessStats}`` on open; the log itself is a read-only ``Mapping`` of
key to (immutable) :class:`AccessStats`. :func:`track_access` wraps any
``MutableMapping`` so reads, writes and deletes are recorded. The log is the
``tracker`` of :class:`xdol.tiering.TieredStore`.

Filesystem atime cannot do this job (under ``relatime`` it is never a count, and any
backup pass resets it), hence an explicit log.

Event schema, one JSON object per line (plain string keys, so a filesystem tracer that
only knows paths can append to the same file, under the lock protocol below)::

    {"k": key, "op": "read"|"write"|"delete"|"admit"|"release"|"pin"|"unpin",
     "n": bytes, "from": "hot"|"cold"|null, "ts": unix_seconds, "tag": str|null}

Only ``k`` and ``op`` are required: an event without ``n`` leaves the size alone.
Replay does not trust file order (several writers may append to one file): it parses
every line (a torn or ill-typed one, e.g. a NaN ``ts`` or a string ``n``, is skipped),
applies the snapshot lines first, then folds the other events in ``(ts, line number)``
order, exactly as live recording folds them. So a ``delete`` clears a key (pin
included) and only later-stamped events bring it back. An event without ``ts`` is
ordered right after the line before it in the file (it takes that line's timestamp,
``0`` at the top of the file): an unstamped ``delete`` still deletes what came before.

Keys must be JSON scalars, in practice ``str``: a key that does not survive a JSON
round trip (a tuple, a ``Path``...) is counted in memory but lost or changed on reopen,
and recording one warns once.

A compacted file holds ``{"k": key, "op": "snapshot", "stats": {...}}`` lines, then one
``{"k": key, "op": "tombstone", "ts": t}`` line per key ever deleted (its latest
delete, see :attr:`AccessLog.tombstones`). Both are the baseline, folded before any
event and not counted as events; a tombstone only records the delete (it never removes
the key: the snapshot is already post-delete). Older readers skip tombstone lines.

Compaction runs by default once the file holds more than ``DFLT_COMPACT_ABOVE`` events
(earlier versions never compacted on their own): it folds away the per-event history
(``tag``, ``from``, each timestamp). Pass ``compact_above=None`` to keep the raw log.

Several processes may share one file. Every append, every replay and every compaction
holds an exclusive advisory lock (``flock``; ``msvcrt.locking`` on Windows) on a sidecar
``<path>.lock`` file, and an appender checks, under that lock, that its handle is still
on the file at ``path`` (reopening it if not), so compaction (which re-folds the file
from disk, then replaces it) never drops another writer's events. An external appender
(e.g. a filesystem tracer that only knows paths) must do the same: take an exclusive
``flock`` on ``<path>.lock`` around each append, opening ``path`` afresh each time; or
else every log of that file must set ``compact_above=None``.

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

import errno
import json
import math
import os
import platform
import time
import warnings
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from types import MappingProxyType
from typing import Any, get_type_hints

from dol import atomic_write

OPS = ("read", "write", "delete", "admit", "release", "pin", "unpin")
_CREATING_OPS = ("read", "write", "pin")  # other ops on an unknown key are no-ops
_BASELINE_OPS = ("snapshot", "tombstone")  # a compacted file's lines, not events
DFLT_COMPACT_ABOVE = 100_000  # events (~10 MB): replaying that many takes well under 1s

_LOCK_UNSUPPORTED = {  # e.g. Lustre without its flock option, some FUSE/SMB mounts
    getattr(errno, name)
    for name in ("ENOLCK", "ENOTSUP", "EOPNOTSUPP", "ENOSYS")
    if hasattr(errno, name)
}

if os.name == "nt":
    import msvcrt

    _LOCK_RETRY_INTERVAL = 0.001  # seconds between attempts on a contended lock

    def _lock(fh) -> None:
        # not LK_LOCK: it sleeps 1s per retry and gives up after ~10s. No timeout:
        # Windows releases a dead process's locks.
        fh.seek(0)
        while True:
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError as e:
                if e.errno not in (errno.EACCES, errno.EDEADLOCK):
                    raise
            time.sleep(_LOCK_RETRY_INTERVAL)

    def _unlock(fh) -> None:
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock(fh) -> None:  # flock: two descriptors exclude each other, even in-process
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    def _unlock(fh) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


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
    admitted_at: float = 0.0  # when the hot copy was admitted; 0.0: unknown or not hot

    @property
    def frequency(self) -> int:
        return self.reads + self.writes


def value_size(v) -> int:
    """``len(v)``, or ``0`` when ``v`` has no length (tracking never raises)."""
    try:
        return len(v)
    except TypeError:
        return 0


def _min_nonzero(a: float, b: float) -> float:
    """The smaller of two timestamps, ``0.0`` meaning unknown."""
    return min((t for t in (a, b) if t), default=0.0)


def _last_event(st: AccessStats) -> float:
    """The latest timestamp ``st`` holds (a key created by ``pin`` has only ``first_seen``)."""
    return max(st.first_seen, st.last_access, st.admitted_at)


def _survives_json(key) -> bool:
    if type(key) is str:
        return True
    try:
        back = json.loads(json.dumps(key))
    except (TypeError, ValueError):
        return False
    return type(back) is type(key) and back == key


def _is_finite(x) -> bool:
    """A finite real number (not a ``bool``, not NaN or infinity)."""
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


_TYPE_CHECKS = {
    int: lambda x: isinstance(x, int) and not isinstance(x, bool),
    float: _is_finite,
    bool: lambda x: isinstance(x, bool),
}
_FIELD_CHECKS = {  # derived from AccessStats: a foreign line must not poison replay
    name: _TYPE_CHECKS[t] for name, t in get_type_hints(AccessStats).items()
}


def _stats_from_snapshot(data: dict) -> AccessStats:
    """``AccessStats`` from a snapshot's dict; unknown fields dropped, bad types rejected."""
    data = {k: v for k, v in data.items() if k in _FIELD_CHECKS}
    for name, v in data.items():  # older versions wrote a float size
        if _FIELD_CHECKS[name] is _TYPE_CHECKS[int] and type(v) is float:
            data[name] = int(v) if _is_finite(v) and v.is_integer() else v
    if not all(_FIELD_CHECKS[k](v) for k, v in data.items()):
        raise TypeError("snapshot field with the wrong type")
    return AccessStats(**data)


def _parse_event(raw: bytes):
    """``(key, op, n, ts, stats)`` from one log line, or ``None`` to skip the line.

    ``k`` and ``op`` are required and ``k`` must be hashable; ``ts`` (absent: ``None``)
    and ``n`` must be finite real numbers when present (a ``tombstone`` needs ``ts``);
    ``stats`` is set only for a ``snapshot`` line. Torn, undecodable and foreign lines
    all give ``None``.
    """
    try:
        ev = json.loads(raw)
        key, op = ev["k"], ev["op"]
        hash(key)
        if op == "snapshot":
            return key, op, None, None, _stats_from_snapshot(ev["stats"])
        ts, n = ev.get("ts"), ev.get("n")
        if op == "tombstone" and _is_finite(ts):
            return key, op, None, ts, None
        if op in OPS and all(x is None or _is_finite(x) for x in (ts, n)):
            return key, op, n, ts, None
    except (
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        OverflowError,
        RecursionError,
    ):  # e.g. a foreign line of 200k nested brackets
        pass
    return None


class _OutOfOrder(Exception):
    """The log is not in replay order: streaming replay gives up for the buffered one."""


def _replay_lines(lines):
    """``(seq, key, op, n, ts, stats)`` for each usable line of ``lines``, in file order.

    Torn, undecodable and foreign lines are skipped. ``ts`` is ``None`` for a snapshot
    line; an event without ``ts`` takes the running max of the timestamps before it
    (baseline lines included, ``0.0`` at the top), so it is ordered right after the
    lines before it.
    """
    running = 0.0
    for seq, raw in enumerate(lines):
        if (ev := _parse_event(raw)) is None:
            continue
        key, op, n, ts, stats = ev
        if op == "snapshot":
            running = max(running, _last_event(stats))
        elif op == "tombstone":
            running = max(running, ts)
        else:
            ts = running if ts is None else ts
            running = max(running, ts)
        yield seq, key, op, n, ts, stats


def _apply_baseline(stats_by_key: dict, tombstones: dict, key, op, ts, stats) -> None:
    """Fold a compacted file's line: a snapshot sets the key's stats, a tombstone only
    raises its tombstone (the snapshot, or its absence, is already post-delete)."""
    if op == "snapshot":
        stats_by_key[key] = stats
    else:
        tombstones[key] = max(ts, tombstones.get(key, ts))


def _fold_streaming(lines, stats_by_key: dict, tombstones: dict) -> tuple[float, int]:
    """Fold ``lines`` already in replay order into ``stats_by_key``, in constant memory.

    Returns ``(last_ts, number_of_events)``. Raises :class:`_OutOfOrder` (leaving
    ``stats_by_key`` partly folded) at a snapshot or tombstone after an event or a
    timestamp going back.
    """
    last_ts, nevents = 0.0, 0
    for _, key, op, n, ts, stats in _replay_lines(lines):
        if op in _BASELINE_OPS:
            if nevents:  # a baseline must be applied before any event
                raise _OutOfOrder
            _apply_baseline(stats_by_key, tombstones, key, op, ts, stats)
            continue
        if ts < last_ts:
            raise _OutOfOrder
        last_ts, nevents = ts, nevents + 1
        _apply(stats_by_key, tombstones, key, op, n, ts)
    return last_ts, nevents


def _fold_buffered(lines, stats_by_key: dict, tombstones: dict) -> tuple[float, int]:
    """Fold ``lines`` in any order: snapshots and tombstones first, then events by
    ``(ts, line)``.

    Returns ``(last_ts, number_of_events)``.
    """
    events = []
    for seq, key, op, n, ts, stats in _replay_lines(lines):
        if op in _BASELINE_OPS:  # the compacted baseline: applied first
            _apply_baseline(stats_by_key, tombstones, key, op, ts, stats)
        else:
            events.append((ts, seq, key, op, n))
    events.sort(key=lambda e: e[:2])
    for ts, _, key, op, n in events:
        _apply(stats_by_key, tombstones, key, op, n, ts)
    # the running max starts at 0.0, as in streaming (and live recording)
    return max([0.0, *(e[0] for e in events)]), len(events)


def _fold_path(path: str) -> tuple[dict, dict, float, int]:
    """``(stats_by_key, tombstones, last_ts, number_of_events)`` replayed from ``path``.

    Streams (constant memory) while the file is already in replay order; the first
    line breaking that order restarts with the buffered fold, which sorts. A missing
    file is an empty log.
    """
    stats_by_key, tombstones = {}, {}
    try:
        with open(path, "rb") as fh:
            last_ts, nevents = _fold_streaming(fh, stats_by_key, tombstones)
    except FileNotFoundError:
        return stats_by_key, tombstones, 0.0, 0
    except _OutOfOrder:
        stats_by_key.clear()
        tombstones.clear()
        with open(path, "rb") as fh:
            last_ts, nevents = _fold_buffered(fh, stats_by_key, tombstones)
    return stats_by_key, tombstones, last_ts, nevents


def _compacted_lines(stats_by_key: Mapping, tombstones: Mapping) -> Iterator[bytes]:
    """The lines of a compacted file: snapshots, then tombstones in timestamp order."""
    for key, st in stats_by_key.items():
        yield _jsonl({"k": key, "op": "snapshot", "stats": asdict(st)})
    for key, ts in sorted(tombstones.items(), key=lambda kv: kv[1]):
        yield _jsonl({"k": key, "op": "tombstone", "ts": ts})


def _jsonl(obj) -> bytes:
    return (json.dumps(obj, default=str) + "\n").encode("utf-8")


# POSIX: an appender keeps its handle, and reopens it when compaction replaced the file.
# Windows cannot replace a file another handle holds open: there, open per append.
_KEEP_APPEND_HANDLE = os.name != "nt"


def _open_for_append(path: str):
    fh = open(path, "a+b")
    if fh.tell():  # a torn last line (crash mid-write) must not eat our event
        fh.seek(-1, os.SEEK_END)
        if fh.read(1) != b"\n":
            fh.write(b"\n")  # append mode: lands at the end whatever the position
    return fh


def _open_lock_file(path: str):
    """``path`` opened for locking, read-only if it cannot be written (``flock`` and
    ``msvcrt.locking`` work on either); ``None`` if it cannot be opened at all."""
    try:
        return open(path, "a+b")
    except PermissionError:
        try:
            return open(path, "rb")
        except OSError:
            return None
    except OSError:
        return None


class _LockedAppender:
    """Appends lines to ``path`` under an exclusive lock on ``<path>.lock``, following
    the file when compaction (under the same lock) replaces it."""

    def __init__(self, path: str):
        self.path = path
        self._fh = None
        self._lock_fh = None
        self._pid = None  # the process the handles belong to

    @contextmanager
    def locked(self):
        """Hold the exclusive lock on ``<path>.lock``; yields whether it is held.

        Not held (the body still runs) when the lock file cannot be opened, e.g. in a
        read-only folder, or the filesystem does not support locking."""
        # handles inherited over a fork share the parent's lock: reopen our own
        if self._pid != os.getpid():
            self.close()
            self._pid = os.getpid()
        if self._lock_fh is None:
            self._lock_fh = _open_lock_file(self.path + ".lock")
        if self._lock_fh is None:
            yield False
            return
        try:
            _lock(self._lock_fh)
        except OSError as e:
            if e.errno not in _LOCK_UNSUPPORTED:
                raise
            yield False
            return
        try:
            yield True
        finally:
            _unlock(self._lock_fh)

    def append(self, line: bytes) -> None:
        """Append ``line`` under the lock, to the file now at ``path``."""
        with self.locked():
            if self._fh is not None and not self._is_on_path():
                self._close_fh()  # compaction replaced the file
            if self._fh is None:
                self._fh = _open_for_append(self.path)
            try:
                self._fh.write(line)
                self._fh.flush()
            finally:
                if not _KEEP_APPEND_HANDLE:
                    self._close_fh()

    def _is_on_path(self) -> bool:
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            return False
        fst = os.fstat(self._fh.fileno())
        return (st.st_ino, st.st_dev) == (fst.st_ino, fst.st_dev)

    def _close_fh(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def close(self) -> None:
        """Close the files (the appender stays usable: it reopens them when needed)."""
        self._close_fh()
        if self._lock_fh is not None:
            self._lock_fh.close()
            self._lock_fh = None


def _apply(stats_by_key: dict, tombstones: dict, key, op, nbytes, ts) -> None:
    """Fold one event into ``stats_by_key`` (shared by live recording and replay).

    ``nbytes=None`` leaves the size alone. Recency is a max, ``first_seen`` a min and
    the size comes from the latest read or write; ``delete`` drops the entry (pin
    included), so a later read or write recreates it unpinned, unless the key has an
    event after the delete's timestamp (then the delete is stale and ignored; live
    recording never meets this: the log starts its clock after every snapshot stamp).
    Replay sorts events by timestamp before folding them, so it gives what live
    recording gave. Every ``delete``, stale or not, raises the key's tombstone
    (``tombstones[key]``, the latest delete's timestamp), which :func:`merge_stats`
    honours.
    """
    if op not in OPS:
        return
    st = stats_by_key.get(key)
    if op == "delete":
        tombstones[key] = max(ts, tombstones.get(key, ts))
        # a delete older than the key's last event (e.g. an old log line replayed over
        # a snapshot) was followed by a re-creation: it must not wipe the key
        if st is None or ts >= _last_event(st):  # same stamp: the delete wins
            stats_by_key.pop(key, None)
        return
    if ts < tombstones.get(key, ts):  # a later delete wiped it (from a compacted
        return  # file: that delete is a tombstone, the key's snapshot is post-delete)
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
            changes["size"] = int(nbytes)  # snapshots hold an int size
        if op == "read":
            changes.update(reads=st.reads + 1, last_read=max(st.last_read, ts))
        else:
            changes.update(writes=st.writes + 1)
        st = replace(st, **changes)
    elif op in ("pin", "unpin"):
        st = replace(st, pinned=op == "pin")
    elif op == "admit":
        st = replace(st, admitted_at=ts)
    elif op == "release":  # frequency history is kept deliberately
        st = replace(st, admitted_at=0.0)
    stats_by_key[key] = st


class AccessLog(Mapping):
    """Append-only JSONL access log, readable as a ``Mapping`` of key -> AccessStats.

    :param path: log file; ``None`` keeps everything in memory (tests).
    :param clock: callable returning unix seconds.
    :param tag: label stamped on each event; defaults to ``platform.node()``.
    :param compact_above: once the file holds more than this many events (lines other
        than snapshots), :meth:`compact` it: checked on open and as this log records
        (counting its own appends since then). ``None``: never; the file then grows with
        every access.

    Opening replays the file in timestamp order (see the module docstring), so the
    stats equal what recording those events live would give. Replay streams in constant
    memory when the file is already in that order (the normal case for one writer) and
    buffers it only when timestamps go back: a log several writers append to is out of
    order, so opening it still costs memory proportional to its length (compaction
    bounds that). Other writers' events are seen on reopen, not live.

    A tracker never raises into a store's read path: if the file cannot be written,
    a warning is issued once and counting continues in memory. Safe across processes,
    forked ones included (see the module docstring); not thread-safe. A context
    manager: leaving it closes the log's files.
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
        compact_above: int | None = DFLT_COMPACT_ABOVE,
    ):
        if compact_above is not None and (
            isinstance(compact_above, bool)
            or not isinstance(compact_above, int)
            or compact_above < 0
        ):
            raise ValueError(
                f"compact_above must be None or a non-negative int, not {compact_above!r}"
            )
        self.path = None if path is None else os.fspath(path)
        self.clock = clock
        self.tag = platform.node() if tag is None else tag
        self.compact_above = compact_above
        self._stats: dict[Any, AccessStats] = {}
        self._tombstones: dict[Any, float] = {}
        self._appender = None if self.path is None else _LockedAppender(self.path)
        self._warned_io = False
        self._warned_key = False
        self._warned_compact = False
        self._last_ts = 0.0  # the clock may step back: events never go back in time
        self._nevents = 0  # events in the file, as far as this log knows
        if self.path is not None and os.path.exists(self.path):
            with self._appender.locked():
                self._stats, self._tombstones, self._last_ts, self._nevents = (
                    _fold_path(self.path)
                )
            # live events must not be stamped before what the baseline already recorded
            self._last_ts = max(
                [self._last_ts, *self._tombstones.values()]
                + [_last_event(st) for st in self._stats.values()]
            )
            self._maybe_compact()

    # -- reading -----------------------------------------------------------
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

    @property
    def tombstones(self) -> Mapping:
        """Read-only ``{key: timestamp of its latest delete}``, for :func:`merge_stats`.

        Kept (and compacted) for every key ever deleted, re-created or not.
        """
        return MappingProxyType(self._tombstones)

    # -- writing -----------------------------------------------------------
    def _warn_once(self, flag: str, message: str) -> None:
        if not getattr(self, flag):
            setattr(self, flag, True)
            warnings.warn(message)

    def record(self, key, op: str, nbytes: int = 0, *, served_from=None) -> None:
        if op not in OPS:
            raise ValueError(f"Unknown op {op!r}; expected one of {OPS}")
        ts = self._last_ts = max(self.clock(), self._last_ts)
        nbytes = int(nbytes) if _is_finite(nbytes) else 0  # a tracker never raises
        _apply(self._stats, self._tombstones, key, op, nbytes, ts)
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
        try:
            self._appender.append(_jsonl(event))
        except OSError as e:
            self._warn_once(
                "_warned_io",
                f"AccessLog cannot write to {self.path!r} ({e}); "
                "counting in memory only.",
            )
            return
        self._nevents += 1
        self._maybe_compact()

    def _maybe_compact(self) -> None:
        """:meth:`compact` past ``compact_above`` events; a failure only warns."""
        if self.compact_above is None or self._nevents <= self.compact_above:
            return
        try:
            self.compact()
        except Exception as e:  # recording (or opening) never raises over compaction
            self._warn_once(
                "_warned_compact",
                f"AccessLog could not compact {self.path!r} ({e}); left as is.",
            )
            self._nevents = 0  # retry only after as many events again

    def compact(self) -> None:
        """Rewrite the file as its folded state: snapshot lines, then tombstone lines.

        Under the lock, so it re-folds the file from disk (keeping every writer's
        events, not only this log's) and replaces it atomically; writers holding the
        old file reopen the new one at their next append. This log's in-memory view
        is left as it was.
        """
        if self.path is None:
            return
        with self._appender.locked() as held:
            if not held:  # unlocked, another writer's events could be lost
                raise OSError(f"cannot lock {self.path}.lock: not compacting")
            if os.path.exists(self.path):
                stats_by_key, tombstones, _, _ = _fold_path(self.path)
                atomic_write(self.path, _compacted_lines(stats_by_key, tombstones))
        self._nevents = 0

    def close(self) -> None:
        """Close the log's files (it stays usable: it reopens them when needed)."""
        if self._appender is not None:
            self._appender.close()

    def __enter__(self) -> AccessLog:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def merge_stats(*logs: Mapping) -> dict:
    """Merge several ``Mapping[key, AccessStats]`` (e.g. logs) into one dict.

    Sums reads/writes, takes the ``size`` and ``admitted_at`` of the log with the
    latest ``last_access`` (the one that saw the key's current state), min
    ``first_seen`` (ignoring unknown zeros), max ``last_access``/``last_read``, OR of
    ``pinned``.

    Deletes are honoured by timestamp: a log's stats of a key whose latest event
    (``first_seen``, ``last_access`` or ``admitted_at``) is not after the latest delete
    of that key in the *other* logs (their ``tombstones``, when they have them) are left
    out, as replaying all the logs as one would drop them. A log's own tombstones never
    apply to its own stats, which are post-delete already. Stats that span a delete and
    a later re-creation count whole.

    >>> old = {'a': AccessStats(reads=3, last_access=5.0)}
    >>> log = AccessLog(clock=lambda: 10.0)
    >>> log.record('a', 'delete')
    >>> 'a' in merge_stats(old, log)
    False
    """
    stones = [getattr(log, "tombstones", {}) for log in logs]
    out: dict[Any, AccessStats] = {}
    for i, log in enumerate(logs):
        for key, st in log.items():
            deleted = max(
                (t[key] for j, t in enumerate(stones) if j != i and key in t),
                default=None,
            )
            if deleted is not None and _last_event(st) <= deleted:
                continue
            cur = out.get(key)
            if cur is None:
                out[key] = st
                continue
            latest = st if st.last_access >= cur.last_access else cur
            out[key] = replace(
                cur,
                reads=cur.reads + st.reads,
                writes=cur.writes + st.writes,
                size=latest.size,
                first_seen=_min_nonzero(cur.first_seen, st.first_seen),
                last_access=max(cur.last_access, st.last_access),
                last_read=max(cur.last_read, st.last_read),
                pinned=cur.pinned or st.pinned,
                admitted_at=latest.admitted_at,
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
