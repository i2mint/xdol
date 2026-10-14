# xdol.memoize

Persistent memoization with record/replay modes, for costly calls.

A transparent wrapper that persists a function’s results in a `MutableMapping` keyed by
a stable hash of the call. It is a VCR cassette for *any* function, not just HTTP: the
first run records, every later run replays from the store – no call, no cost, no network.
The driving use is tests of code that calls paid APIs (LLMs, image/video generation),
which must stay free and fast, but it serves any expensive, deterministic-enough call.

```pycon
>>> import tempfile
>>> calls = []
>>> @memoize_calls(store=tempfile.mkdtemp(), mode="record")
... def slow_square(x):
...     calls.append(x)
...     return {"result": x * x}
>>> slow_square(3), slow_square(3)
({'result': 9}, {'result': 9})
>>> calls
[3]
```

**Replay is the default; recording is an opt-in.** Under the default `mode=None` a call
whose key is not in the store raises [`CacheMiss`](#xdol.memoize.CacheMiss) instead of calling the function,
unless the run sets the `XDOL_MEMOIZE_RECORD` environment variable (see
[`recording_enabled()`](#xdol.memoize.recording_enabled)). A key that quietly moved would otherwise read as a normal,
billable test run. Spending is an act that carries a flag. For a plain memoizer that
records on a miss, pass `mode="record"` (as above).

Four pieces, each a keyword argument with a good default (a *seam*):

- `key_func` – input to key; default [`xdol.keys.robust_key()`](xdol.keys.html.md#xdol.keys.robust_key) (see also `drop=`);
- `codec` – result to bytes; `"json"` (default), `"text"`, `"bytes"` for binary
  assets, or `"paired"` for a `(metadata, asset_bytes)` result stored as
  `<key>.json` + `<key>.bin` (see [`xdol.x_codecs`](xdol.x_codecs.html.md#module-xdol.x_codecs));
- `store` – a directory path (a stable, committable folder: see
  [`file_cache_store()`](#xdol.memoize.file_cache_store)), or any `MutableMapping`. Pass a bytes store such as
  > `s3dol.s3_store(bucket)` and the codec is applied on top, so swapping the filesystem
  > for S3 changes nothing at the call site;
- `mode` – `"replay"`, `"record"`, `"refresh"` or `"off"`; see `MODES`.

**Cache the top-level composite**: wrap the whole `prompt -> JSON -> asset URL ->
downloaded bytes` pipeline, not each intermediate API step, so one intent maps to one
final result. Nothing enforces this; it keeps the surface area (and the number of
cassettes to review) small.

The wrapper preserves the function’s signature and metadata (`functools.wraps`) and
supports coroutine functions. Exceptions raised by the function are never cached.

### Module Attributes

| [`RECORD_ENV`](#xdol.memoize.RECORD_ENV)          | Set to a truthy value to let `mode=None` calls record on a miss.                                                                                                       |
|----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`TRUTHY_ENV_VALUES`](#xdol.memoize.TRUTHY_ENV_VALUES)   | Environment-variable spellings that count as "on".                                                                                                                     |
| [`DFLT_CORRUPT_ERRORS`](#xdol.memoize.DFLT_CORRUPT_ERRORS) | Errors that mean "this stored entry cannot be decoded" (as opposed to a store/network failure, which must never be mistaken for a missing entry and cost a real call). |

### Functions

| [`file_cache_store`](#xdol.memoize.file_cache_store)(rootdir, \*[, codec])            | A persistent, atomic, codec-aware `MutableMapping` over a directory.   |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`memoize_calls`](#xdol.memoize.memoize_calls)([func, codec, key_func, drop, ...]) | Persistently memoize `func` into `store` (record/replay).              |
| [`recording_enabled`](#xdol.memoize.recording_enabled)([env])                          | True when the environment variable `env` is set to a truthy value.     |

### Classes

| [`AtomicFiles`](#xdol.memoize.AtomicFiles)(\*args[, delete_func])   | A bytes store over a directory whose writes are atomic.   |
|---------------------------------------------------------------------------------------|-----------------------------------------------------------|

### Exceptions

| [`CacheCorrupt`](#xdol.memoize.CacheCorrupt)                     | A stored entry exists but cannot be decoded (replay mode refuses to guess).       |
|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`CacheMiss`](#xdol.memoize.CacheMiss)                        | A memoized call missed its store while recording was disabled.                    |
| [`CacheWriteError`](#xdol.memoize.CacheWriteError)(message, result) | The function ran but its result could not be stored (e.g. not JSON-serializable). |

### *class* xdol.memoize.AtomicFiles(\*args, delete_func=<built-in function remove>, \*\*kwargs)

Bases: `Files`

A bytes store over a directory whose writes are atomic.

Plain file writes can leave a truncated cassette if the process dies mid-write, which
then fails to decode (or worse, decodes). Here each value is written to a temporary
sibling and renamed into place (`os.replace`), and temporary files are invisible to
iteration. Missing sub-directories of a key (`"a/b"`) are created.

### *exception* xdol.memoize.CacheCorrupt

Bases: [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError)

A stored entry exists but cannot be decoded (replay mode refuses to guess).

### *exception* xdol.memoize.CacheMiss

Bases: [`LookupError`](https://docs.python.org/3/builtins/exceptions.html#LookupError)

A memoized call missed its store while recording was disabled.

The alternative to this exception is a real, possibly billable call nobody asked
for. Two things cause it: an input the store has genuinely never seen, and a cache
**key** that moved under an unchanged input – usually because the key was computed
over something incidental (a rendered global registry, a timestamp, a path).

### *exception* xdol.memoize.CacheWriteError(message, result)

Bases: [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError)

The function ran but its result could not be stored (e.g. not JSON-serializable).

The result is not lost: it is the `result` attribute.

### xdol.memoize.DFLT_CORRUPT_ERRORS *= (<class 'xdol.x_codecs.CodecDecodeError'>,)*

Errors that mean “this stored entry cannot be decoded” (as opposed to a store/network
failure, which must never be mistaken for a missing entry and cost a real call).

### xdol.memoize.RECORD_ENV *= 'XDOL_MEMOIZE_RECORD'*

Set to a truthy value to let `mode=None` calls record on a miss.

### xdol.memoize.TRUTHY_ENV_VALUES *= frozenset({'1', 'on', 'true', 'yes'})*

Environment-variable spellings that count as “on”.

### xdol.memoize.file_cache_store(rootdir, , codec='json')

A persistent, atomic, codec-aware `MutableMapping` over a directory.

Keys are filenames, values are decoded objects. `rootdir` is
created if missing and is meant to be a stable folder you commit (a cassette
directory), not a temp dir. `codec="paired"` stores `(metadata, asset_bytes)`
values as `<key>.json` + `<key>.bin`.

* **Return type:**
  [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)

```pycon
>>> import tempfile
>>> store = file_cache_store(tempfile.mkdtemp())
>>> store["k"] = {"a": 1}
>>> store["k"], list(store)
({'a': 1}, ['k'])
```

### xdol.memoize.memoize_calls(func=None, \*, store, codec=None, key_func=None, drop=(), mode=None, default_mode='replay', record_env='XDOL_MEMOIZE_RECORD', corrupt_errors=(<class 'xdol.x_codecs.CodecDecodeError'>, ), on_write_error='raise', copy_results=None, reread_after_write=True)

Persistently memoize `func` into `store` (record/replay).

Usable as `memoize_calls(fn, store=...)` or as a decorator
`@memoize_calls(store=...)`.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – the (costly) function; wrap the *composite* top-level call.
  * **store** – a directory path (-> [`file_cache_store()`](#xdol.memoize.file_cache_store)), or a `MutableMapping`.
  * **codec** – `None` (default) means a path gets `"json"` and a mapping is used as
    it is (its values are the results). Otherwise `store` is a *bytes* mapping
    and this is applied on top: `"json"`, `"text"`, `"bytes"`,
    `"paired"`, a [`xdol.x_codecs.Codec`](xdol.x_codecs.html.md#xdol.x_codecs.Codec), or an `(encode, decode)` pair.
  * **key_func** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`...`](https://docs.python.org/3/builtins/constants.html#Ellipsis), [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – input to key. Default [`xdol.keys.robust_key()`](xdol.keys.html.md#xdol.keys.robust_key).
  * **drop** ([`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – parameter names of `func` to leave out of the key (callbacks, loggers,
    `self`); builds the key with [`xdol.keys.make_key_func()`](xdol.keys.html.md#xdol.keys.make_key_func), which also
    binds calls to the signature. Not combinable with `key_func`.
  * **mode** (`Union`[[`Literal`](https://docs.python.org/3/library/typing.html#typing.Literal)[`'replay'`, `'record'`, `'refresh'`, `'off'`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[], [`Literal`](https://docs.python.org/3/library/typing.html#typing.Literal)[`'replay'`, `'record'`, `'refresh'`, `'off'`]], [`None`](https://docs.python.org/3/builtins/constants.html#None)]) – one of `MODES`, a zero-argument callable returning one (evaluated on
    every call), or `None` (default) for: `"record"` if the `record_env`
    variable is truthy, else `default_mode`. Read at call time, so setting the
    variable after import (e.g. in a pytest hook) works.
  * **default_mode** ([`Literal`](https://docs.python.org/3/library/typing.html#typing.Literal)[`'replay'`, `'record'`, `'refresh'`, `'off'`]) – what `mode=None` means when the environment does not opt in.
    `"replay"` (default) makes spending an explicit act.
  * **record_env** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – name of the opt-in environment variable.
  * **corrupt_errors** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`type`](https://docs.python.org/3/builtins/functions.html#type)[[`BaseException`](https://docs.python.org/3/builtins/exceptions.html#BaseException)], [`...`](https://docs.python.org/3/builtins/constants.html#Ellipsis)]) – exception types raised by reading an entry that mean “undecodable”
    (default: [`xdol.x_codecs.CodecDecodeError`](xdol.x_codecs.html.md#xdol.x_codecs.CodecDecodeError), which the codecs raise;
    add your own for an object-valued store that can fail to load). In replay
    mode they raise [`CacheCorrupt`](#xdol.memoize.CacheCorrupt); when recording, the entry is recomputed
    and overwritten, with a warning. Any other error – a network failure, a
    misconfigured store – propagates and never costs a real call.
  * **copy_results** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – deep-copy values read from the store, so callers cannot mutate the
    cache. `None` (default) means: yes when `store` is an object-valued
    mapping used as is (a codec makes a fresh object per read anyway).
  * **reread_after_write** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – after recording, return the value as read back from the store,
    so a recording run and a replay run return *the same thing* (JSON turns tuples
    into lists and int keys into strings). Turn off to skip the extra read of a
    large asset.
  * **on_write_error** ([`Literal`](https://docs.python.org/3/library/typing.html#typing.Literal)[`'raise'`, `'warn'`]) – what to do if a fresh result cannot be stored: `"raise"`
    [`CacheWriteError`](#xdol.memoize.CacheWriteError) (carrying the result) or `"warn"` and return it.
* **Return type:**
  [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)

The returned wrapper has attributes `cache_store`, `key_func`, `cache_key(\*args,
\*\*kwargs)` and `cache_mode()` (the mode that would apply now).

### xdol.memoize.recording_enabled(env='XDOL_MEMOIZE_RECORD')

True when the environment variable `env` is set to a truthy value.

The opt-in is environment-level rather than per-call, so “this run may spend” is one
visible fact about the run instead of a property of each test.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

```pycon
>>> recording_enabled("XDOL_SURELY_UNSET_VARIABLE")
False
```
