"""A bounded hot store over a cold source of truth (write-through, GDSF release).

``TieredStore(hot, cold, budget_bytes=...)`` is a ``MutableMapping``: writes go to
``cold`` first, then ``hot``; reads are served from ``hot`` when present, else from
``cold`` (and admitted to ``hot`` when the policy says so). Placement is decided by a
:class:`PlacementPolicy` (default :class:`GdsfPolicy`: Greedy-Dual-Size-Frequency with
aging, Cherkasova 1998) from the statistics of an :class:`xdol.access.AccessLog`.

Verbs are free functions, store first: :func:`sweep`, :func:`pin`, :func:`unpin`,
:func:`status`, :func:`prefetch`, :func:`local_paths`.

>>> hot, cold = {}, {}
>>> store = TieredStore(hot, cold, budget_bytes=None, tracker=AccessLog())
>>> store['a'] = b'hello'
>>> cold['a'], hot['a']
(b'hello', b'hello')
>>> status(store)['hot_bytes']
5
"""

from __future__ import annotations

import os
import time
import warnings
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from xdol.access import AccessLog, AccessStats, default_access_log_for


class TieringError(Exception):
    """Base class of tiering errors."""


class HotWriteError(TieringError):
    """The hot write failed after the cold store accepted the record.

    The record is safe in cold; do not retry the cold write.
    """


class PlacementPolicy(Protocol):
    """Decides what enters and leaves the hot tier."""

    high_watermark: float
    low_watermark: float

    def admit(self, key, stats: AccessStats | None, *, now: float) -> bool: ...

    def to_release(
        self,
        hot_stats: Mapping[Any, AccessStats],
        *,
        hot_bytes: int,
        budget_bytes: int,
        now: float,
    ) -> Iterable: ...


@dataclass(kw_only=True)
class GdsfPolicy:
    """Greedy-Dual-Size-Frequency with aging, plus pins, minimum residency and
    second-hit admission.

    Priority ``H = L + F * cost(size) / size`` with ``F = reads + writes``,
    ``cost(size) = cost_per_request + cost_per_byte * size`` and ``L`` the H of the last
    released key (per process; resets at restart).
    """

    high_watermark: float = 0.85
    low_watermark: float = 0.75
    min_residency_s: float = 7 * 86400
    admit_after_reads: int = 2
    admit_window_s: float = 30 * 86400
    cost_per_request: float = 1.0
    cost_per_byte: float = 0.0
    _aging: float = field(default=0.0, init=False, repr=False)

    def admit(self, key, stats: AccessStats | None, *, now: float) -> bool:
        return (
            stats is not None
            and stats.reads >= self.admit_after_reads - 1
            and now - stats.last_read <= self.admit_window_s
        )

    def priority(self, stats: AccessStats) -> float:
        size = max(stats.size, 1)
        cost = self.cost_per_request + self.cost_per_byte * size
        return self._aging + stats.frequency * cost / size

    def is_protected(self, stats: AccessStats, *, now: float) -> bool:
        return stats.pinned or now - stats.last_access < self.min_residency_s

    def to_release(
        self,
        hot_stats: Mapping[Any, AccessStats],
        *,
        hot_bytes: int,
        budget_bytes: int,
        now: float,
    ) -> Iterator:
        if hot_bytes <= self.high_watermark * budget_bytes:
            return
        ranked = sorted(
            (
                (self.priority(st), k)
                for k, st in hot_stats.items()
                if not self.is_protected(st, now=now)
            ),
            key=lambda pk: pk[0],
        )
        for h, k in ranked:
            # Lazy: the consumer stops pulling at the low watermark, so the aging
            # value reflects the last key actually released.
            yield k
            self._aging = h


@dataclass
class SweepReport:
    keys_released: list
    bytes_before: int
    bytes_after: int


class TieredStore(MutableMapping):
    """Bounded hot store over a cold source of truth. See the module docstring.

    :param budget_bytes: ``None``: hot is a full mirror (never released, every read miss
        admitted); ``0``: cold-only; ``> 0``: bounded.
    """

    def __init__(
        self,
        hot: MutableMapping,
        cold: MutableMapping,
        *,
        budget_bytes: int | None,
        policy: PlacementPolicy | None = None,
        tracker: AccessLog | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self._hot, self._cold = hot, cold
        self._budget = budget_bytes
        self._policy = GdsfPolicy() if policy is None else policy
        self._tracker = default_access_log_for(hot) if tracker is None else tracker
        self._clock = clock
        self._hot_sizes: dict = {}
        self._last_sweep: SweepReport | None = None
        for k in list(hot):
            st = self._tracker.stats(k)
            if st is None:
                size = len(hot[k])
                self._tracker.record(k, "write", size)
            else:
                size = st.size
            self._hot_sizes[k] = size

    hot = property(lambda self: self._hot)
    cold = property(lambda self: self._cold)
    policy = property(lambda self: self._policy)
    tracker = property(lambda self: self._tracker)
    budget_bytes = property(lambda self: self._budget)

    # -- private bookkeeping ------------------------------------------------
    @property
    def _hot_bytes(self) -> int:
        return sum(self._hot_sizes.values())

    def _put_hot(self, k, v) -> None:
        self._hot[k] = v
        self._hot_sizes[k] = len(v)

    def _drop_hot(self, k) -> None:
        self._hot.pop(k, None)
        self._hot_sizes.pop(k, None)

    def _check_release(self) -> None:
        if (
            self._budget
            and self._hot_bytes > self._policy.high_watermark * self._budget
        ):
            sweep(self)

    # -- MutableMapping -----------------------------------------------------
    def __setitem__(self, k, v):
        self._cold[k] = v
        if self._budget != 0:
            try:
                self._put_hot(k, v)
            except Exception as e:
                self._drop_hot(k)
                self._tracker.record(k, "write", len(v))
                raise HotWriteError(k) from e
        self._tracker.record(k, "write", len(v))
        self._check_release()

    def __delitem__(self, k):
        del self._cold[k]
        self._drop_hot(k)
        self._tracker.record(k, "delete", 0)

    def __getitem__(self, k):
        now = self._clock()
        served_from = None
        if k in self._hot:
            try:
                v, served_from = self._hot[k], "hot"
            except KeyError:
                self._hot_sizes.pop(k, None)
        if served_from is None:
            v, served_from = self._cold[k], "cold"
            if self._admits(k, now):
                self._admit(k, v)
        self._tracker.record(k, "read", len(v), served_from=served_from)
        self._check_release()
        return v

    def _admits(self, k, now: float) -> bool:
        if self._budget is None:
            return True
        if self._budget == 0:
            return False
        return self._policy.admit(k, self._tracker.stats(k), now=now)

    def _admit(self, k, v) -> None:
        try:
            self._put_hot(k, v)
        except Exception as e:
            self._drop_hot(k)
            warnings.warn(f"Could not admit {k!r} to the hot tier: {e!r}")

    def __contains__(self, k):
        return k in self._hot or k in self._cold

    def __iter__(self):
        return iter(self._cold)

    def __len__(self):
        return len(self._cold)


# -- verbs ---------------------------------------------------------------------


def sweep(store: TieredStore) -> SweepReport:
    """Release hot copies, lowest priority first, down to the low watermark.

    Never touches ``cold``. Warns if every remaining key is protected.
    """
    before = store._hot_bytes
    budget = store._budget
    released: list = []
    if budget is not None:
        target = store._policy.low_watermark * budget
        hot_stats = {
            k: store._tracker.stats(k) or AccessStats(size=size)
            for k, size in store._hot_sizes.items()
        }
        reached = store._hot_bytes <= target
        candidates = store._policy.to_release(
            hot_stats,
            hot_bytes=before,
            budget_bytes=budget,
            now=store._clock(),
        )
        for k in candidates:
            if store._hot_bytes <= target:
                reached = True
                break
            store._drop_hot(k)
            store._tracker.record(k, "release", 0)
            released.append(k)
        else:
            reached = store._hot_bytes <= target
        if not reached and before > store._policy.high_watermark * budget:
            warnings.warn(
                "sweep could not reach the low watermark: remaining keys are protected."
            )
    report = SweepReport(released, before, store._hot_bytes)
    store._last_sweep = report
    return report


def pin(store: TieredStore, key) -> None:
    """Protect ``key``'s hot copy from release (persisted in the tracker)."""
    store._tracker.record(key, "pin", 0)


def unpin(store: TieredStore, key) -> None:
    store._tracker.record(key, "unpin", 0)


def prefetch(store: TieredStore, keys: Iterable) -> None:
    """Admit ``keys`` to hot regardless of policy."""
    for k in keys:
        if k not in store._hot_sizes:
            store._put_hot(k, store._cold[k])
    store._check_release()


def status(store: TieredStore) -> dict:
    last = store._last_sweep
    return {
        "hot_bytes": store._hot_bytes,
        "budget_bytes": store._budget,
        "hot_keys": len(store._hot_sizes),
        "cold_keys": len(store._cold),
        "pinned": sorted(
            (k for k in store._tracker if store._tracker.stats(k).pinned), key=str
        ),
        "last_sweep": None if last is None else vars(last).copy(),
    }


class _LocalPaths(Mapping):
    def __init__(self, store: TieredStore):
        self._store = store

    def _path(self, key) -> str:
        hot = self._store._hot
        if hasattr(hot, "_id_of_key"):
            return hot._id_of_key(key)
        return os.path.join(hot.rootdir, key)

    def __getitem__(self, key) -> str:
        prefetch(self._store, [key])
        return self._path(key)

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)


def local_paths(store: TieredStore) -> Mapping:
    """Read-only ``{key: real file path}``; materialises each key into ``hot``.

    Requires ``hot`` to expose ``rootdir`` (raises ``TypeError`` otherwise).
    """
    if getattr(store._hot, "rootdir", None) is None:
        raise TypeError("local_paths requires a hot store with a 'rootdir'")
    return _LocalPaths(store)
