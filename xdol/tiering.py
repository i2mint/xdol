"""A bounded hot store over a cold source of truth (write-through, GDSF release).

``TieredStore(hot, cold, budget_bytes=...)`` is a ``MutableMapping``: writes go to
``cold`` first, then ``hot``; reads are served from ``hot`` when present, else from
``cold`` (and admitted to ``hot`` when the policy says so). Placement is decided by a
:class:`PlacementPolicy` (default :class:`GdsfPolicy`: Greedy-Dual-Size-Frequency with
aging, Cherkasova 1998) from the statistics of an :class:`xdol.access.AccessLog`, whose
clock is the store's clock.

Verbs are free functions, store first: :func:`sweep`, :func:`pin`, :func:`unpin`,
:func:`status`, :func:`prefetch`, :func:`local_paths`.

Values must support ``len`` (their size in bytes): ``__setitem__`` raises ``TypeError``
before writing anything otherwise. A value read from ``cold`` without ``len`` counts
as size 0. Single-threaded: there is no locking.

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
import warnings
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Any, Protocol

from dol import path_of as dol_path_of
from xdol.access import AccessLog, AccessStats, default_access_log_for, value_size


class TieringError(Exception):
    """Base class of tiering errors."""


class HotWriteError(TieringError):
    """The hot write failed after the cold store accepted the record.

    The record is safe in cold; do not retry the cold write.
    """


def _exceeds_watermark(
    hot_bytes: int, budget_bytes: int | None, watermark: float
) -> bool:
    """Whether ``hot_bytes > watermark * budget_bytes``; never when unbounded."""
    return budget_bytes is not None and hot_bytes > watermark * budget_bytes


class PlacementPolicy(Protocol):
    """Decides what enters and leaves the hot tier."""

    high_watermark: float
    low_watermark: float

    def admit(self, key, stats: AccessStats | None, *, now: float) -> bool: ...

    def on_hot_access(self, key, stats: AccessStats, *, admitted: bool) -> None:
        """``key`` was accessed in hot; ``admitted``: it was not there before."""

    def on_hot_drop(self, key) -> None:
        """``key`` left hot (released, deleted or overwritten with no hot copy)."""

    def to_release(
        self,
        hot_stats: Mapping[Any, AccessStats],
        *,
        hot_bytes: int,
        budget_bytes: int,
        now: float,
    ) -> Iterable: ...


@dataclass(kw_only=True)
class _ResidencyPolicy:
    """What GDSF and LRU share: watermarks, second-hit admission, pins and minimum residency.

    Subclasses decide the *order* of release in ``to_release``.
    """

    high_watermark: float = 0.85
    low_watermark: float = 0.75
    min_residency_s: float = 7 * 86400
    admit_after_reads: int = 2
    admit_window_s: float = 30 * 86400

    def admit(self, key, stats: AccessStats | None, *, now: float) -> bool:
        if self.admit_after_reads <= 1:
            return True
        return (
            stats is not None
            and stats.reads >= self.admit_after_reads - 1
            and now - stats.last_read <= self.admit_window_s
        )

    def on_hot_access(self, key, stats: AccessStats, *, admitted: bool) -> None:
        pass

    def on_hot_drop(self, key) -> None:
        pass

    def is_protected(self, stats: AccessStats, *, now: float) -> bool:
        # residency counts from admission; unknown (e.g. admitted before it was logged)
        # falls back to the last access
        since = stats.admitted_at or stats.last_access
        return stats.pinned or now - since < self.min_residency_s

    def _release_order(
        self, candidates: list[tuple[Any, AccessStats]], *, now: float
    ) -> Iterator:
        """Keys of the unprotected ``(key, stats)`` candidates, first to release first."""
        raise NotImplementedError

    def to_release(
        self,
        hot_stats: Mapping[Any, AccessStats],
        *,
        hot_bytes: int,
        budget_bytes: int,
        now: float,
    ) -> Iterator:
        if not _exceeds_watermark(hot_bytes, budget_bytes, self.high_watermark):
            return
        self._forget_unknown(hot_stats.keys())
        candidates = [
            (k, st) for k, st in hot_stats.items() if not self.is_protected(st, now=now)
        ]
        yield from self._release_order(candidates, now=now)

    def _forget_unknown(self, hot_keys) -> None:
        """Hook: drop per-key policy state for keys no longer hot."""


@dataclass(kw_only=True)
class LruPolicy(_ResidencyPolicy):
    """Least-recently-used release: the simple alternative to :class:`GdsfPolicy`.

    Once hot bytes pass the high watermark, unprotected hot keys are released
    oldest-``last_access`` first (the consumer stops at the low watermark).
    """

    def _release_order(self, candidates, *, now: float) -> Iterator:
        yield from (k for k, _ in sorted(candidates, key=lambda ks: ks[1].last_access))


@dataclass(kw_only=True)
class GdsfPolicy(_ResidencyPolicy):
    """Greedy-Dual-Size-Frequency with aging, plus pins, minimum residency and
    second-hit admission.

    Each hot key carries ``H = L + F * cost(size) / size``, fixed when it is accessed,
    with ``F`` its accesses since admission to hot, ``cost(size) = cost_per_request +
    cost_per_byte * size`` and ``L`` the H of the last released key at that time
    (per process; resets at restart). Releasing the lowest H raises ``L``, so a key
    not accessed for a while loses to newer ones however often it was read. A key
    already hot when this process first sees it starts fresh, ``F = 1``, as if just
    admitted: its logged history (often long, all of it older than ``L``'s reset)
    would otherwise keep it favoured after every restart. A key not accessed during
    this process (a cron ``sweep``) is ranked on its recency instead: ``F`` is
    ``0.5 ** (age / seed_half_life_s)``, ``age`` being the time since its
    ``last_access``, so among such keys of equal size the least recently used goes first (a key seeded this way and then accessed continues from ``F + 1``).
    """

    cost_per_request: float = 1.0
    cost_per_byte: float = 0.0
    seed_half_life_s: float | None = 30 * 86400  # None: seed F = 1, ignoring recency
    _aging: float = field(default=0.0, init=False, repr=False, compare=False)
    _entries: dict = field(
        default_factory=dict, init=False, repr=False, compare=False
    )  # k: (F, H)

    def __post_init__(self):
        if self.seed_half_life_s is not None and not self.seed_half_life_s > 0:
            raise ValueError(
                f"seed_half_life_s must be None or positive, not {self.seed_half_life_s!r}"
            )

    def priority(self, frequency: float, size: int) -> float:
        """H of a key accessed now with this frequency and size."""
        size = max(size, 1)
        cost = self.cost_per_request + self.cost_per_byte * size
        return self._aging + frequency * cost / size

    def _set(self, key, frequency: float, size: int) -> float:
        h = self.priority(frequency, size)
        self._entries[key] = (frequency, h)
        return h

    def on_hot_access(self, key, stats: AccessStats, *, admitted: bool) -> None:
        if admitted:
            f = 1
        elif key in self._entries:
            f = self._entries[key][0] + 1
        else:
            f = 1  # first seen by this process: fresh, as if just admitted
        self._set(key, f, stats.size)

    def on_hot_drop(self, key) -> None:
        self._entries.pop(key, None)

    def _seed_frequency(self, stats: AccessStats, *, now: float) -> float:
        """``F`` of a hot key this process has not seen accessed: decays with its age."""
        if self.seed_half_life_s is None:
            return 1
        # unknown recency (last_access 0) is the oldest: F near 0, as LRU
        return 0.5 ** (max(now - stats.last_access, 0.0) / self.seed_half_life_s)

    def _h(self, key, stats: AccessStats, *, now: float) -> float:
        entry = self._entries.get(key)
        if entry:
            return entry[1]
        return self._set(key, self._seed_frequency(stats, now=now), stats.size)

    def _forget_unknown(self, hot_keys) -> None:
        for k in self._entries.keys() - hot_keys:
            del self._entries[k]

    def _release_order(self, candidates, *, now: float) -> Iterator:
        ranked = sorted(
            ((self._h(k, st, now=now), k) for k, st in candidates), key=lambda hk: hk[0]
        )
        for h, k in ranked:
            # Lazy: the consumer stops pulling at the low watermark, so L reflects
            # the last key actually released.
            yield k
            self._aging = max(self._aging, h)


@dataclass
class SweepReport:
    keys_released: list
    bytes_before: int
    bytes_after: int


def _file_size(hot, key) -> int | None:
    """Size of ``key``'s file when ``hot`` is file-backed, without reading it."""
    try:
        return os.path.getsize(dol_path_of(hot, key))
    except (OSError, TypeError, KeyError):
        return None


def _require_size(v) -> int:
    try:
        return len(v)
    except TypeError:
        raise TypeError(
            "TieredStore values must support len() (their size in bytes); "
            f"got {type(v).__name__}"
        ) from None


class TieredStore(MutableMapping):
    """Bounded hot store over a cold source of truth. See the module docstring.

    :param budget_bytes: ``None``: hot is a full mirror (never released, every read miss
        admitted); ``0``: cold-only, no hot tier (writes skip hot and read misses are not admitted, but only
        :func:`sweep` enforces it: :func:`prefetch` and :func:`local_paths` still fill
        hot until the next sweep, which releases every hot copy, protected or not);
        ``> 0``: bounded.
    :param policy: a :class:`PlacementPolicy`; default :class:`GdsfPolicy`.
    :param tracker: an :class:`AccessLog` (its ``clock`` is the store's clock); default
        :func:`xdol.access.default_access_log_for` ``(hot)``.
    :param max_item_fraction: in ``(0, 1]``; with a bounded budget, an item larger than
        ``max_item_fraction * budget_bytes`` is never held hot by the store itself: a
        read miss is served from cold without admission, and a write goes to cold only
        (dropping any stale hot copy). The default ``1.0`` keeps only items larger than
        the whole budget out. :func:`prefetch` and :func:`local_paths` are explicit
        requests and are exempt.

    Keys already in ``hot`` at construction are not recorded as accesses. Their size
    comes from the tracker, else from the file size when ``hot`` is file-backed, else
    counts as 0 until the key's next hot hit.
    """

    def __init__(
        self,
        hot: MutableMapping,
        cold: MutableMapping,
        *,
        budget_bytes: int | None,
        policy: PlacementPolicy | None = None,
        tracker: AccessLog | None = None,
        max_item_fraction: float = 1.0,
    ):
        if not 0 < max_item_fraction <= 1:
            raise ValueError(
                f"max_item_fraction must be in (0, 1], not {max_item_fraction!r}"
            )
        self._hot, self._cold = hot, cold
        self._budget = budget_bytes
        self._max_item_fraction = max_item_fraction
        self._policy = GdsfPolicy() if policy is None else policy
        self._tracker = default_access_log_for(hot) if tracker is None else tracker
        self._hot_sizes: dict = {}
        self._hot_bytes = 0
        self._last_sweep: SweepReport | None = None
        self._warned_protected = False
        for k in list(hot):
            st = self._tracker.stats(k)
            known = st.size if st is not None else 0
            self._set_size(k, known or _file_size(hot, k) or 0)

    hot = property(lambda self: self._hot)
    cold = property(lambda self: self._cold)
    policy = property(lambda self: self._policy)
    tracker = property(lambda self: self._tracker)
    budget_bytes = property(lambda self: self._budget)
    max_item_fraction = property(lambda self: self._max_item_fraction)

    # -- private bookkeeping ------------------------------------------------
    def _now(self) -> float:
        return self._tracker.clock()

    def _set_size(self, k, size: int) -> None:
        self._hot_bytes += size - self._hot_sizes.get(k, 0)
        self._hot_sizes[k] = size

    def _put_hot(self, k, v, size: int) -> bool:
        """Write ``k`` to hot; return whether it was newly admitted."""
        admitted = k not in self._hot_sizes
        self._hot[k] = v
        self._set_size(k, size)
        return admitted

    def _drop_hot(self, k, *, record_release: bool = True) -> bool:
        """Drop ``k``'s hot copy; return whether there was one.

        A dropped copy is logged as a ``release`` (``admitted_at`` means "hot since"),
        unless ``record_release`` is false (a delete, which logs its own event).
        """
        try:
            del self._hot[k]  # not .pop: MutableMapping.pop reads the value first
        except KeyError:
            pass
        was_hot = k in self._hot_sizes
        self._hot_bytes -= self._hot_sizes.pop(k, 0)
        self._policy.on_hot_drop(k)
        if was_hot and record_release:
            self._tracker.record(k, "release", 0)
        return was_hot

    def _too_big(self, size: int) -> bool:
        """Whether an item of ``size`` bytes is never held hot (``max_item_fraction``)."""
        return bool(self._budget) and size > self._max_item_fraction * self._budget

    def _touched(self, k, *, admitted: bool) -> None:
        if admitted:
            self._tracker.record(k, "admit", 0)
        self._policy.on_hot_access(k, self._tracker.stats(k), admitted=admitted)

    def _check_release(self, *, exclude: Iterable = ()) -> None:
        if self._budget and _exceeds_watermark(
            self._hot_bytes, self._budget, self._policy.high_watermark
        ):
            sweep(self, exclude=exclude)

    # -- MutableMapping -----------------------------------------------------
    def __setitem__(self, k, v):
        size = _require_size(v)
        self._cold[k] = v
        try:
            if self._budget == 0 or self._too_big(size):
                self._drop_hot(k)  # never leave a stale hot copy behind
                admitted = None
            else:
                admitted = self._put_hot(k, v, size)
        except Exception as e:
            self._tracker.record(k, "write", size)
            self._drop_hot(k)
            raise HotWriteError(k) from e
        self._tracker.record(k, "write", size)
        if admitted is not None:
            self._touched(k, admitted=admitted)
        self._check_release()

    def __delitem__(self, k):
        # Hot first: a failure then leaves the record in cold, never a deleted
        # record still served from hot.
        was_hot = self._drop_hot(k, record_release=False)
        try:
            del self._cold[k]
        except BaseException:
            if was_hot:  # the hot copy is gone but the key is not: log the release
                self._tracker.record(k, "release", 0)
            raise
        self._tracker.record(k, "delete", 0)

    def __getitem__(self, k):
        now = self._now()
        served_from, admitted = None, False
        if k in self._hot:
            try:
                v, served_from = self._hot[k], "hot"
            except KeyError:
                pass
        if served_from is None:
            if k in self._hot_sizes:
                self._drop_hot(k)  # the hot copy vanished behind our back: forget it
            v, served_from = self._cold[k], "cold"
        size = value_size(v)
        if served_from == "hot":
            self._set_size(k, size)  # also learns sizes unknown at startup
        elif self._admits(k, now, size=size):
            admitted = self._admit(k, v, size)
        self._tracker.record(k, "read", size, served_from=served_from)
        if served_from == "hot" or admitted:
            self._touched(k, admitted=admitted)
        self._check_release()
        return v

    def _admits(self, k, now: float, *, size: int) -> bool:
        if self._budget is None:
            return True
        if self._budget == 0 or self._too_big(size):
            return False
        return self._policy.admit(k, self._tracker.stats(k), now=now)

    def _admit(self, k, v, size: int) -> bool:
        try:
            return self._put_hot(k, v, size)
        except Exception as e:
            self._drop_hot(k)
            warnings.warn(f"Could not admit {k!r} to the hot tier: {e!r}")
            return False

    def __contains__(self, k):
        return k in self._hot or k in self._cold

    def __iter__(self):
        return iter(self._cold)

    def __len__(self):
        return len(self._cold)


# -- verbs ---------------------------------------------------------------------


def _release(store: TieredStore, key, released: list) -> None:
    store._drop_hot(key)  # logs the release
    released.append(key)


def _warn_once_per_stuck_episode(store: TieredStore, stuck: bool) -> None:
    """Warn when a sweep gets stuck on protected keys, once until it gets unstuck."""
    if stuck and not store._warned_protected:
        warnings.warn(
            "sweep could not reach the low watermark: remaining keys are protected."
        )
    store._warned_protected = stuck


def sweep(store: TieredStore, *, exclude: Iterable = ()) -> SweepReport:
    """Release hot copies, lowest priority first, down to the low watermark.

    Never touches ``cold``, nor the keys in ``exclude``. Warns if every remaining key
    is protected, once per stuck episode (until a sweep reaches it again). With ``budget_bytes=0`` (no hot tier) every hot copy is released,
    protected or not.
    """
    before = store._hot_bytes
    budget = store._budget
    released: list = []
    if budget == 0:  # no hot tier: release everything, whatever the policy protects
        exclude = set(exclude)
        for k in [k for k in store._hot_sizes if k not in exclude]:
            _release(store, k, released)
    elif budget is not None:
        target = store._policy.low_watermark * budget
        exclude = set(exclude)
        hot_stats = {}
        for k, size in store._hot_sizes.items():
            st = store._tracker.stats(k) or AccessStats(size=size)
            # excluded keys are presented to the policy as pinned
            hot_stats[k] = replace(st, pinned=True) if k in exclude else st
        candidates = store._policy.to_release(
            hot_stats, hot_bytes=before, budget_bytes=budget, now=store._now()
        )
        for k in candidates:
            if store._hot_bytes <= target:
                break
            _release(store, k, released)
        stuck = store._hot_bytes > target and _exceeds_watermark(
            before, budget, store._policy.high_watermark
        )
        _warn_once_per_stuck_episode(store, stuck)
    report = SweepReport(released, before, store._hot_bytes)
    store._last_sweep = report
    return report


def pin(store: TieredStore, key) -> None:
    """Protect ``key``'s hot copy from release (persisted in the tracker)."""
    store._tracker.record(key, "pin", 0)


def unpin(store: TieredStore, key) -> None:
    store._tracker.record(key, "unpin", 0)


def prefetch(store: TieredStore, keys: Iterable) -> None:
    """Admit ``keys`` to hot regardless of policy, as reads served from cold.

    An explicit request: exempt from ``max_item_fraction`` too.

    The release this may trigger never releases ``keys``.
    """
    keys = list(keys)
    for k in keys:
        if k not in store._hot_sizes:
            v = store._cold[k]
            size = value_size(v)
            store._put_hot(k, v, size)
            store._tracker.record(k, "read", size, served_from="cold")
            store._touched(k, admitted=True)
    store._check_release(exclude=keys)


def status(store: TieredStore) -> dict:
    last = store._last_sweep
    return {
        "hot_bytes": store._hot_bytes,
        "budget_bytes": store._budget,
        "hot_keys": len(store._hot_sizes),
        "cold_keys": len(store._cold),
        "pinned": sorted((k for k, st in store._tracker.items() if st.pinned), key=str),
        "last_sweep": None if last is None else vars(last).copy(),
    }


class _LocalPaths(Mapping):
    def __init__(self, store: TieredStore, path_of: Callable[[Any], str]):
        self._store = store
        self._path_of = path_of

    def __contains__(self, key) -> bool:
        return key in self._store  # checks hot/cold; never fetches

    def __getitem__(self, key) -> str:
        store = self._store
        path = self._path_of(key)  # before any fetch: a bad path_of costs nothing
        if key in store._hot_sizes and not os.path.exists(path):
            store._drop_hot(key)  # deleted behind our back: refetch from cold
        was_hot = key in store._hot_sizes
        prefetch(store, [key])
        if not os.path.exists(path):
            raise FileNotFoundError(f"{key!r} was not materialised at {path!r}")
        if (
            was_hot
        ):  # using the file is an access (recency/frequency), though nothing is read
            size = os.path.getsize(path)
            store._set_size(key, size)
            store._tracker.record(key, "read", size, served_from="hot")
            store._touched(key, admitted=False)
        return path

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)


def local_paths(
    store: TieredStore, *, path_of: Callable[[Any], str] | None = None
) -> Mapping:
    """Read-only ``{key: real file path}``; materialises each key into ``hot``.

    Materialisation is an explicit request, like :func:`prefetch`: exempt from the
    policy and from ``max_item_fraction``.

    ``path_of(key)`` gives the file path of ``key`` in ``hot``; the default is
    :func:`dol.path_of` on the hot store, which follows wrapped layers (``TypeError`` on access if ``hot`` is not
    file-backed, before anything is fetched; ``KeyError`` for a key leaving its root).
    ``FileNotFoundError`` is raised if the file is not there after materialisation.
    ``key in paths`` checks the store without fetching.
    """
    if path_of is None:
        path_of = partial(dol_path_of, store._hot)
    return _LocalPaths(store, path_of)
