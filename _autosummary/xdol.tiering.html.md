# xdol.tiering

A bounded hot store over a cold source of truth (write-through, GDSF release).

`TieredStore(hot, cold, budget_bytes=...)` is a `MutableMapping`: writes go to
`cold` first, then `hot`; reads are served from `hot` when present, else from
`cold` (and admitted to `hot` when the policy says so). Placement is decided by a
[`PlacementPolicy`](#xdol.tiering.PlacementPolicy) (default [`GdsfPolicy`](#xdol.tiering.GdsfPolicy): Greedy-Dual-Size-Frequency with
aging, Cherkasova 1998) from the statistics of an [`xdol.access.AccessLog`](xdol.access.html.md#xdol.access.AccessLog).

Verbs are free functions, store first: [`sweep()`](#xdol.tiering.sweep), [`pin()`](#xdol.tiering.pin), `unpin()`,
`status()`, [`prefetch()`](#xdol.tiering.prefetch), [`local_paths()`](#xdol.tiering.local_paths).

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

| [`local_paths`](#xdol.tiering.local_paths)(store)    | Read-only `{key: real file path}`; materialises each key into `hot`.   |
|------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`pin`](#xdol.tiering.pin)(store, key)       | Protect `key`'s hot copy from release (persisted in the tracker).      |
| [`prefetch`](#xdol.tiering.prefetch)(store, keys) | Admit `keys` to hot regardless of policy.                              |
| `status`(store)                                                        |                                                                        |
| [`sweep`](#xdol.tiering.sweep)(store)          | Release hot copies, lowest priority first, down to the low watermark.  |
| `unpin`(store, key)                                                    |                                                                        |

### Classes

| [`GdsfPolicy`](#xdol.tiering.GdsfPolicy)(\*[, high_watermark, ...])           | Greedy-Dual-Size-Frequency with aging, plus pins, minimum residency and second-hit admission.   |
|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| [`PlacementPolicy`](#xdol.tiering.PlacementPolicy)(\*args, \*\*kwargs)             | Decides what enters and leaves the hot tier.                                                    |
| [`SweepReport`](#xdol.tiering.SweepReport)(keys_released, bytes_before, ...)   |                                                                                                 |
| [`TieredStore`](#xdol.tiering.TieredStore)(hot, cold, \*, budget_bytes[, ...]) | Bounded hot store over a cold source of truth.                                                  |

### Exceptions

| [`HotWriteError`](#xdol.tiering.HotWriteError)   | The hot write failed after the cold store accepted the record.   |
|------------------------------------------------------------------|------------------------------------------------------------------|
| [`TieringError`](#xdol.tiering.TieringError)    | Base class of tiering errors.                                    |

### *class* xdol.tiering.GdsfPolicy(, high_watermark=0.85, low_watermark=0.75, min_residency_s=604800, admit_after_reads=2, admit_window_s=2592000, cost_per_request=1.0, cost_per_byte=0.0)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Greedy-Dual-Size-Frequency with aging, plus pins, minimum residency and
second-hit admission.

Priority `H = L + F * cost(size) / size` with `F = reads + writes`,
`cost(size) = cost_per_request + cost_per_byte * size` and `L` the H of the last
released key (per process; resets at restart).

### *exception* xdol.tiering.HotWriteError

Bases: [`TieringError`](#xdol.tiering.TieringError)

The hot write failed after the cold store accepted the record.

The record is safe in cold; do not retry the cold write.

### *class* xdol.tiering.PlacementPolicy(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

Decides what enters and leaves the hot tier.

### *class* xdol.tiering.SweepReport(keys_released, bytes_before, bytes_after)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

### *class* xdol.tiering.TieredStore(hot, cold, \*, budget_bytes, policy=None, tracker=None, clock=<built-in function time>)

Bases: [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)

Bounded hot store over a cold source of truth. See the module docstring.

* **Parameters:**
  **budget_bytes** ([`int`](https://docs.python.org/3/builtins/functions.html#int) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – `None`: hot is a full mirror (never released, every read miss
  admitted); `0`: cold-only; `> 0`: bounded.

### *exception* xdol.tiering.TieringError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base class of tiering errors.

### xdol.tiering.local_paths(store)

Read-only `{key: real file path}`; materialises each key into `hot`.

Requires `hot` to expose `rootdir` (raises `TypeError` otherwise).

* **Return type:**
  [`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)

### xdol.tiering.pin(store, key)

Protect `key`’s hot copy from release (persisted in the tracker).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### xdol.tiering.prefetch(store, keys)

Admit `keys` to hot regardless of policy.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### xdol.tiering.sweep(store)

Release hot copies, lowest priority first, down to the low watermark.

Never touches `cold`. Warns if every remaining key is protected.

* **Return type:**
  [`SweepReport`](#xdol.tiering.SweepReport)
