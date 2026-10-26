# xdol.access

Persisted, mergeable per-key access statistics for any store.

An [`AccessLog`](#xdol.access.AccessLog) is an append-only JSONL file of access events, folded into an
in-memory `{key: AccessStats}` on open; the log itself is a read-only `Mapping` of
key to (immutable) [`AccessStats`](#xdol.access.AccessStats). [`track_access()`](#xdol.access.track_access) wraps any
`MutableMapping` so reads, writes and deletes are recorded. The log is the
`tracker` of [`xdol.tiering.TieredStore`](xdol.tiering.html.md#xdol.tiering.TieredStore).

Filesystem atime cannot do this job (under `relatime` it is never a count, and any
backup pass resets it), hence an explicit log.

Event schema, one JSON object per line (plain string keys, so a filesystem tracer that
only knows paths can append to the same file):

```default
{"k": key, "op": "read"|"write"|"delete"|"release"|"pin"|"unpin",
 "n": bytes, "from": "hot"|"cold"|null, "ts": unix_seconds, "tag": str|null}
```

Only `k` and `op` are required: an event without `n` leaves the size alone.
Replay does not trust file order (several writers may append to one file): it parses
every line (a torn or ill-typed one, e.g. a NaN `ts` or a string `n`, is skipped),
applies the snapshot lines first, then folds the other events in `(ts, line number)`
order, exactly as live recording folds them. So a `delete` clears a key (pin
included) and only later-stamped events bring it back. An event without `ts` is
ordered right after the line before it in the file (it takes that line’s timestamp,
`0` at the top of the file): an unstamped `delete` still deletes what came before.

Keys must be JSON scalars, in practice `str`: a key that does not survive a JSON
round trip (a tuple, a `Path`…) is counted in memory but lost or changed on reopen,
and recording one warns once.

A compacted file holds `{"k": key, "op": "snapshot", "stats": {...}}` lines, folded as
initial state (the baseline the events after it build on).

```pycon
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
```

### Functions

| [`default_access_log_for`](#xdol.access.default_access_log_for)(store, \*\*kwargs)      | An [`AccessLog`](#xdol.access.AccessLog) beside `store`'s `rootdir` (never inside it).   |
|-------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| [`merge_stats`](#xdol.access.merge_stats)(\*logs)                            | Merge several `Mapping[key, AccessStats]` (e.g. logs) into one dict.                                          |
| [`track_access`](#xdol.access.track_access)(store, \*[, tracker, clock, tag]) | Wrap `store` so reads, writes and deletes are recorded in `tracker`.                                          |
| [`value_size`](#xdol.access.value_size)(v)                                  | `len(v)`, or `0` when `v` has no length (tracking never raises).                                              |

### Classes

| [`AccessLog`](#xdol.access.AccessLog)([path, clock, tag])           | Append-only JSONL access log, readable as a `Mapping` of key -> AccessStats.   |
|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`AccessStats`](#xdol.access.AccessStats)([size, reads, writes, ...]) | Access statistics of one key (immutable: update with `dataclasses.replace`).   |

### *class* xdol.access.AccessLog(path=None, \*, clock=<built-in function time>, tag=None)

Bases: [`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)

Append-only JSONL access log, readable as a `Mapping` of key -> AccessStats.

* **Parameters:**
  * **path** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`PathLike`](https://docs.python.org/3/library/os.html#os.PathLike) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – log file; `None` keeps everything in memory (tests).
  * **clock** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[], [`float`](https://docs.python.org/3/builtins/functions.html#float)]) – callable returning unix seconds.
  * **tag** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – label stamped on each event; defaults to `platform.node()`.

Opening replays the file in timestamp order (see the module docstring), so the
stats equal what recording those events live would give. A tracker never raises
into a store’s read path: if the file cannot be written,
a warning is issued once and counting continues in memory. Not thread-safe.

#### compact()

Rewrite the file as one snapshot line per key, atomically.

Safe only when no other writer has the file open: another process appending
to it keeps writing to the replaced file, and those events are lost.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### stats(key)

Alias of `self.get(key)`.

* **Return type:**
  [`AccessStats`](#xdol.access.AccessStats) | [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* xdol.access.AccessStats(size=0, reads=0, writes=0, first_seen=0.0, last_access=0.0, last_read=0.0, pinned=False)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Access statistics of one key (immutable: update with `dataclasses.replace`).

### xdol.access.default_access_log_for(store, \*\*kwargs)

An [`AccessLog`](#xdol.access.AccessLog) beside `store`’s `rootdir` (never inside it).

Without a `rootdir`, an in-memory log, with a warning.

* **Return type:**
  [`AccessLog`](#xdol.access.AccessLog)

### xdol.access.merge_stats(\*logs)

Merge several `Mapping[key, AccessStats]` (e.g. logs) into one dict.

Sums reads/writes, takes the `size` of the latest access, min `first_seen`
(ignoring unknown zeros), max `last_access`/`last_read`, OR of `pinned`.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)

### xdol.access.track_access(store, , tracker=None, clock=None, tag=None)

Wrap `store` so reads, writes and deletes are recorded in `tracker`.

`__contains__`, `__iter__` and `__len__` are never recorded. Without a
`tracker`, [`default_access_log_for()`](#xdol.access.default_access_log_for) picks one, built with `clock` and
`tag` (passing either alongside a `tracker` is an error: set them on the
log). A value without `len` is recorded with size 0. The wrapped store is
available as `.store` and the log as `.tracker`.

* **Return type:**
  [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)

### xdol.access.value_size(v)

`len(v)`, or `0` when `v` has no length (tracking never raises).

* **Return type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)
