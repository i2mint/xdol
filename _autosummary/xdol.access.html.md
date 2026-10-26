# xdol.access

Persisted, mergeable per-key access statistics for any store.

An [`AccessLog`](#xdol.access.AccessLog) is an append-only JSONL file of access events, folded into an
in-memory `{key: AccessStats}` on open. [`track_access()`](#xdol.access.track_access) wraps any
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

A compacted file holds `{"k": key, "op": "snapshot", "stats": {...}}` lines, folded as
initial state.

```pycon
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
```

### Functions

| [`default_access_log_for`](#xdol.access.default_access_log_for)(store, \*\*kwargs)      | An [`AccessLog`](#xdol.access.AccessLog) beside `store`'s `rootdir` (never inside it).   |
|-------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| [`merge_stats`](#xdol.access.merge_stats)(\*logs)                            | Merge several logs (or `{key: AccessStats}` mappings) into one dict.                                          |
| [`track_access`](#xdol.access.track_access)(store, \*[, tracker, clock, tag]) | Wrap `store` so reads, writes and deletes are recorded in `tracker`.                                          |

### Classes

| [`AccessLog`](#xdol.access.AccessLog)([path, clock, tag])           | Append-only JSONL access log with an in-memory aggregate.   |
|------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| [`AccessStats`](#xdol.access.AccessStats)([size, reads, writes, ...]) | Access statistics of one key.                               |

### *class* xdol.access.AccessLog(path=None, \*, clock=<built-in function time>, tag=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Append-only JSONL access log with an in-memory aggregate.

* **Parameters:**
  * **path** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`PathLike`](https://docs.python.org/3/library/os.html#os.PathLike) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – log file; `None` keeps everything in memory (tests).
  * **clock** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[], [`float`](https://docs.python.org/3/builtins/functions.html#float)]) – callable returning unix seconds.
  * **tag** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – label stamped on each event; defaults to `platform.node()`.

A tracker never raises into a store’s read path: if the file cannot be written,
a warning is issued once and counting continues in memory.

#### compact()

Rewrite the file as one snapshot line per key, atomically.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* xdol.access.AccessStats(size=0, reads=0, writes=0, first_seen=0.0, last_access=0.0, last_read=0.0, pinned=False)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Access statistics of one key.

### xdol.access.default_access_log_for(store, \*\*kwargs)

An [`AccessLog`](#xdol.access.AccessLog) beside `store`’s `rootdir` (never inside it).

Without a `rootdir`, an in-memory log, with a warning.

* **Return type:**
  [`AccessLog`](#xdol.access.AccessLog)

### xdol.access.merge_stats(\*logs)

Merge several logs (or `{key: AccessStats}` mappings) into one dict.

Sums reads/writes, takes the latest `size`, min `first_seen`, max
`last_access`/`last_read`, OR of `pinned`.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)

### xdol.access.track_access(store, \*, tracker=None, clock=<built-in function time>, tag=None)

Wrap `store` so reads, writes and deletes are recorded in `tracker`.

`__contains__`, `__iter__` and `__len__` are never recorded. Without a
`tracker`, [`default_access_log_for()`](#xdol.access.default_access_log_for) picks one. The wrapped store is
available as `.store` and the log as `.tracker`.

* **Return type:**
  [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)
