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
only knows paths can append to the same file, under the lock protocol below):

```default
{"k": key, "op": "read"|"write"|"delete"|"admit"|"release"|"pin"|"unpin",
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

Folding a compacted prefix plus the events after it is exact when those events are
stamped after the compacted ones (the normal case). A *late* event, stamped before some
compacted events (a slow writer that stamped it, then waited for the lock while another
process compacted; or a clock running behind), folds only approximately: a snapshot
keeps no per-event history, so it cannot split its counts around a late, older
`delete`, nor order a late `pin`/`admit` against a compacted `unpin`/`release`.
Per-field stamps would fix the flag ordering; only a grace window (compacting events
older than some age, keeping the recent tail raw) fixes the counts, and only for events
less than that window late. Revisit (i2mint/xdol#44) if a multi-writer user shows the
drift matters.

Keys must be JSON scalars, in practice `str`: a key that does not survive a JSON
round trip (a tuple, a `Path`…) is counted in memory but lost or changed on reopen,
and recording one warns once.

A compacted file holds `{"k": key, "op": "snapshot", "stats": {...}}` lines, then one
`{"k": key, "op": "tombstone", "ts": t}` line per key ever deleted (its latest
delete, see [`AccessLog.tombstones`](#xdol.access.AccessLog.tombstones)). Both are the baseline, folded before any
event and not counted as events; a tombstone only records the delete (it never removes
the key: the snapshot is already post-delete). Older readers skip tombstone lines.

A log built with `keep_tombstones_for=seconds` drops tombstones older than that at
compaction (default: keep them forever).

Compaction runs by default once the file holds more than `DFLT_COMPACT_ABOVE` events
(earlier versions never compacted on their own): it folds away the per-event history
(`tag`, `from`, each timestamp). Pass `compact_above=None` to keep the raw log.

Several processes may share one file. Every append, every replay and every compaction
holds an exclusive advisory lock (`flock`; `msvcrt.locking` on Windows) on a sidecar
`<path>.lock` file, and an appender checks, under that lock, that its handle is still
on the file at `path` (reopening it if not), so compaction (which re-folds the file
from disk, then replaces it) never drops another writer’s events. An external appender
(e.g. a filesystem tracer that only knows paths) must do the same: take an exclusive
`flock` on `<path>.lock` around each append, opening `path` afresh each time; or
else every log of that file must set `compact_above=None`.

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

| [`AccessLog`](#xdol.access.AccessLog)([path, clock, tag, compact_above, ...])   | Append-only JSONL access log, readable as a `Mapping` of key -> AccessStats.   |
|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`AccessStats`](#xdol.access.AccessStats)([size, reads, writes, ...])             | Access statistics of one key (immutable: update with `dataclasses.replace`).   |

### *class* xdol.access.AccessLog(path=None, \*, clock=<built-in function time>, tag=None, compact_above=100000, keep_tombstones_for=None)

Bases: [`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)

Append-only JSONL access log, readable as a `Mapping` of key -> AccessStats.

* **Parameters:**
  * **path** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`PathLike`](https://docs.python.org/3/library/os.html#os.PathLike) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – log file; `None` keeps everything in memory (tests).
  * **clock** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[], [`float`](https://docs.python.org/3/builtins/functions.html#float)]) – callable returning unix seconds.
  * **tag** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – label stamped on each event; defaults to `platform.node()`.
  * **compact_above** ([`int`](https://docs.python.org/3/builtins/functions.html#int) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – once the file holds more than this many events (lines other
    than snapshots), [`compact()`](#xdol.access.AccessLog.compact) it: checked on open and as this log records
    (counting its own appends since then). `None`: never; the file then grows with
    every access.
  * **keep_tombstones_for** ([`float`](https://docs.python.org/3/builtins/functions.html#float) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – 

    seconds a delete’s tombstone is kept: [`compact()`](#xdol.access.AccessLog.compact)
    drops those older than `clock() - keep_tombstones_for`, from the file and
    from this log’s memory. `None` (default): keep them forever, so a churning
    store’s tombstones grow without bound. A tombstone stops two things from
    resurrecting a deleted key: stats of another log merged by [`merge_stats()`](#xdol.access.merge_stats),
    and a late event of this file (see the module docstring) stamped before the
    delete. Keep them longer than any merged log can lag and any writer can be
    late; `0` drops them all at every compaction. Pruning happens only when
    [`compact()`](#xdol.access.AccessLog.compact) runs (automatically only for a file log with `compact_above`
    > set). Retention is per file: whichever log compacts applies its own setting to
    > every writer’s tombstones, so let all logs sharing a file use the same one.

Opening replays the file in timestamp order (see the module docstring), so the
stats equal what recording those events live would give. Replay streams in constant
memory when the file is already in that order (the normal case for one writer) and
buffers it only when timestamps go back: a log several writers append to is out of
order, so opening it still costs memory proportional to its length (compaction
bounds that). Other writers’ events are seen on reopen, not live.

A tracker never raises into a store’s read path: if the file cannot be written,
a warning is issued once and counting continues in memory. Safe across processes,
forked ones included (see the module docstring); not thread-safe. A context
manager: leaving it closes the log’s files.

#### close()

Close the log’s files (it stays usable: it reopens them when needed).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### compact()

Rewrite the file as its folded state: snapshot lines, then tombstone lines.

Under the lock, so it re-folds the file from disk (keeping every writer’s
events, not only this log’s) and replaces it atomically; writers holding the
old file reopen the new one at their next append. This log’s in-memory stats
are left as they were; its tombstones older than `keep_tombstones_for` are
dropped, as from the file.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### stats(key)

Alias of `self.get(key)`.

* **Return type:**
  [`AccessStats`](#xdol.access.AccessStats) | [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### *property* tombstones *: [Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)*

Read-only `{key: timestamp of its latest delete}`, for [`merge_stats()`](#xdol.access.merge_stats).

Kept (and compacted) for every key ever deleted, re-created or not.

### *class* xdol.access.AccessStats(size=0, reads=0, writes=0, first_seen=0.0, last_access=0.0, last_read=0.0, pinned=False, admitted_at=0.0)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Access statistics of one key (immutable: update with `dataclasses.replace`).

### xdol.access.default_access_log_for(store, \*\*kwargs)

An [`AccessLog`](#xdol.access.AccessLog) beside `store`’s `rootdir` (never inside it).

Without a `rootdir`, an in-memory log, with a warning.

* **Return type:**
  [`AccessLog`](#xdol.access.AccessLog)

### xdol.access.merge_stats(\*logs)

Merge several `Mapping[key, AccessStats]` (e.g. logs) into one dict.

Sums reads/writes, takes the `size` and `admitted_at` of the log with the
latest `last_access` (the one that saw the key’s current state), min
`first_seen` (ignoring unknown zeros), max `last_access`/`last_read`, OR of
`pinned`.

Deletes are honoured by timestamp: a log’s stats of a key whose latest event
(`first_seen`, `last_access` or `admitted_at`) is not after the latest delete
of that key in the *other* logs (their `tombstones`, when they have them) are left
out, as replaying all the logs as one would drop them. A log’s own tombstones never
apply to its own stats, which are post-delete already. Stats that span a delete and
a later re-creation count whole.

```pycon
>>> old = {'a': AccessStats(reads=3, last_access=5.0)}
>>> log = AccessLog(clock=lambda: 10.0)
>>> log.record('a', 'delete')
>>> 'a' in merge_stats(old, log)
False
```

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
