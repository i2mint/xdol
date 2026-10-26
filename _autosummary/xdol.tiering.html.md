# xdol.tiering

A bounded hot store over a cold source of truth (write-through, GDSF release).

`TieredStore(hot, cold, budget_bytes=...)` is a `MutableMapping`: writes go to
`cold` first, then `hot`; reads are served from `hot` when present, else from
`cold` (and admitted to `hot` when the policy says so). Placement is decided by a
[`PlacementPolicy`](#xdol.tiering.PlacementPolicy) (default [`GdsfPolicy`](#xdol.tiering.GdsfPolicy): Greedy-Dual-Size-Frequency with
aging, Cherkasova 1998) from the statistics of an [`xdol.access.AccessLog`](xdol.access.html.md#xdol.access.AccessLog), whose
clock is the store’s clock.

Verbs are free functions, store first: [`sweep()`](#xdol.tiering.sweep), [`pin()`](#xdol.tiering.pin), `unpin()`,
`status()`, [`prefetch()`](#xdol.tiering.prefetch), [`local_paths()`](#xdol.tiering.local_paths).

Values must support `len` (their size in bytes): `__setitem__` raises `TypeError`
before writing anything otherwise. A value read from `cold` without `len` counts
as size 0. Single-threaded: there is no locking.

```pycon
>>> hot, cold = {}, {}
>>> store = TieredStore(hot, cold, budget_bytes=None, tracker=AccessLog())
>>> store['a'] = b'hello'
>>> cold['a'], hot['a']
(b'hello', b'hello')
>>> status(store)['hot_bytes']
5
```

### Functions

| [`local_paths`](#xdol.tiering.local_paths)(store, \*[, path_of])   | Read-only `{key: real file path}`; materialises each key into `hot`.   |
|--------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`pin`](#xdol.tiering.pin)(store, key)                     | Protect `key`'s hot copy from release (persisted in the tracker).      |
| [`prefetch`](#xdol.tiering.prefetch)(store, keys)               | Admit `keys` to hot regardless of policy, as reads served from cold.   |
| `status`(store)                                                                      |                                                                        |
| [`sweep`](#xdol.tiering.sweep)(store, \*[, exclude])         | Release hot copies, lowest priority first, down to the low watermark.  |
| `unpin`(store, key)                                                                  |                                                                        |

### Classes

| [`GdsfPolicy`](#xdol.tiering.GdsfPolicy)(\*[, high_watermark, ...])           | Greedy-Dual-Size-Frequency with aging, plus pins, minimum residency and second-hit admission.                       |
|--------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| [`LruPolicy`](#xdol.tiering.LruPolicy)(\*[, high_watermark, ...])            | Least-recently-used release: the simple alternative to [`GdsfPolicy`](#xdol.tiering.GdsfPolicy). |
| [`PlacementPolicy`](#xdol.tiering.PlacementPolicy)(\*args, \*\*kwargs)             | Decides what enters and leaves the hot tier.                                                                        |
| [`SweepReport`](#xdol.tiering.SweepReport)(keys_released, bytes_before, ...)   |                                                                                                                     |
| [`TieredStore`](#xdol.tiering.TieredStore)(hot, cold, \*, budget_bytes[, ...]) | Bounded hot store over a cold source of truth.                                                                      |

### Exceptions

| [`HotWriteError`](#xdol.tiering.HotWriteError)   | The hot write failed after the cold store accepted the record.   |
|------------------------------------------------------------------|------------------------------------------------------------------|
| [`TieringError`](#xdol.tiering.TieringError)    | Base class of tiering errors.                                    |

### *class* xdol.tiering.GdsfPolicy(, high_watermark=0.85, low_watermark=0.75, min_residency_s=604800, admit_after_reads=2, admit_window_s=2592000, cost_per_request=1.0, cost_per_byte=0.0, seed_half_life_s=2592000)

Bases: `_ResidencyPolicy`

Greedy-Dual-Size-Frequency with aging, plus pins, minimum residency and
second-hit admission.

Each hot key carries `H = L + F * cost(size) / size`, fixed when it is accessed,
with `F` its accesses since admission to hot, `cost(size) = cost_per_request +
cost_per_byte * size` and `L` the H of the last released key at that time
(per process; resets at restart). Releasing the lowest H raises `L`, so a key
not accessed for a while loses to newer ones however often it was read. A key
already hot when this process first sees it starts fresh, `F = 1`, as if just
admitted: its logged history (often long, all of it older than `L`’s reset)
would otherwise keep it favoured after every restart. A key not accessed during
this process (a cron `sweep`) is ranked on its recency instead: `F` is
`0.5 ** (age / seed_half_life_s)`, `age` being the time since its
`last_access`, so among such keys of equal size the least recently used goes first (a key seeded this way and then accessed continues from `F + 1`).

#### priority(frequency, size)

H of a key accessed now with this frequency and size.

* **Return type:**
  [`float`](https://docs.python.org/3/builtins/functions.html#float)

### *exception* xdol.tiering.HotWriteError

Bases: [`TieringError`](#xdol.tiering.TieringError)

The hot write failed after the cold store accepted the record.

The record is safe in cold; do not retry the cold write.

### *class* xdol.tiering.LruPolicy(, high_watermark=0.85, low_watermark=0.75, min_residency_s=604800, admit_after_reads=2, admit_window_s=2592000)

Bases: `_ResidencyPolicy`

Least-recently-used release: the simple alternative to [`GdsfPolicy`](#xdol.tiering.GdsfPolicy).

Once hot bytes pass the high watermark, unprotected hot keys are released
oldest-`last_access` first (the consumer stops at the low watermark).

### *class* xdol.tiering.PlacementPolicy(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

Decides what enters and leaves the hot tier.

#### on_hot_access(key, stats, , admitted)

`key` was accessed in hot; `admitted`: it was not there before.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### on_hot_drop(key)

`key` left hot (released, deleted or overwritten with no hot copy).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* xdol.tiering.SweepReport(keys_released, bytes_before, bytes_after)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

### *class* xdol.tiering.TieredStore(hot, cold, , budget_bytes, policy=None, tracker=None, max_item_fraction=1.0)

Bases: [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)

Bounded hot store over a cold source of truth. See the module docstring.

* **Parameters:**
  * **budget_bytes** ([`int`](https://docs.python.org/3/builtins/functions.html#int) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – 

    `None`: hot is a full mirror (never released, every read miss
    admitted); `0`: cold-only, no hot tier (writes skip hot and read misses are not admitted, but only
    [`sweep()`](#xdol.tiering.sweep) enforces it: [`prefetch()`](#xdol.tiering.prefetch) and [`local_paths()`](#xdol.tiering.local_paths) still fill
    > hot until the next sweep, which releases every hot copy, protected or not);
    > `> 0`: bounded.
  * **policy** ([`PlacementPolicy`](#xdol.tiering.PlacementPolicy) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – a [`PlacementPolicy`](#xdol.tiering.PlacementPolicy); default [`GdsfPolicy`](#xdol.tiering.GdsfPolicy).
  * **tracker** ([`AccessLog`](xdol.access.html.md#xdol.access.AccessLog) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – an `AccessLog` (its `clock` is the store’s clock); default
    [`xdol.access.default_access_log_for()`](xdol.access.html.md#xdol.access.default_access_log_for) `(hot)`.
  * **max_item_fraction** ([`float`](https://docs.python.org/3/builtins/functions.html#float)) – in `(0, 1]`; with a bounded budget, an item larger than
    `max_item_fraction * budget_bytes` is never held hot by the store itself: a
    read miss is served from cold without admission, and a write goes to cold only
    (dropping any stale hot copy). The default `1.0` keeps only items larger than
    the whole budget out. [`prefetch()`](#xdol.tiering.prefetch) and [`local_paths()`](#xdol.tiering.local_paths) are explicit
    requests and are exempt.

Keys already in `hot` at construction are not recorded as accesses. Their size
comes from the tracker, else from the file size when `hot` is file-backed, else
counts as 0 until the key’s next hot hit.

### *exception* xdol.tiering.TieringError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base class of tiering errors.

### xdol.tiering.local_paths(store, , path_of=None)

Read-only `{key: real file path}`; materialises each key into `hot`.

Materialisation is an explicit request, like [`prefetch()`](#xdol.tiering.prefetch): exempt from the
policy and from `max_item_fraction`.

`path_of(key)` gives the file path of `key` in `hot`; the default is
`dol.path_of()` on the hot store, which follows wrapped layers (`TypeError` on access if `hot` is not
file-backed, before anything is fetched; `KeyError` for a key leaving its root).
`FileNotFoundError` is raised if the file is not there after materialisation.
`key in paths` checks the store without fetching.

* **Return type:**
  [`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)

### xdol.tiering.pin(store, key)

Protect `key`’s hot copy from release (persisted in the tracker).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### xdol.tiering.prefetch(store, keys)

Admit `keys` to hot regardless of policy, as reads served from cold.

An explicit request: exempt from `max_item_fraction` too.

The release this may trigger never releases `keys`.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### xdol.tiering.sweep(store, , exclude=())

Release hot copies, lowest priority first, down to the low watermark.

Never touches `cold`, nor the keys in `exclude`. Warns if every remaining key
is protected, once per stuck episode (until a sweep reaches it again). With `budget_bytes=0` (no hot tier) every hot copy is released,
protected or not.

* **Return type:**
  [`SweepReport`](#xdol.tiering.SweepReport)
