# Persistent record/replay memoizer

`xdol.memoize_calls` wraps a costly function so that the first run **records** its result to a store and every later run **replays** it: no call, no cost, no network. It is a VCR cassette for any function, not just HTTP. This page walks through the use case that motivated it: tests that exercise real paid APIs.

## The 30-second version

```python
from pathlib import Path
from xdol import memoize_calls

CASSETTES = Path(__file__).parent / "cassettes"   # anchored: see "Where the folder is" below

@memoize_calls(store=CASSETTES)                # a stable folder you commit
def plan_video(prompt: str, *, logger=None) -> dict:
    ...  # calls an LLM, then an image API, then downloads the asset

plan_video("a cat on a skateboard")            # replay-only by default: a miss raises CacheMiss
```

Where the folder is: a relative path such as `"tests/cassettes"` is resolved against the working directory *when the function is decorated* (import time). Running pytest from another directory then looks in, and in record mode creates, a different folder. Anchor it on `__file__` as above.

Recording is an explicit act: run the tests once with `XDOL_MEMOIZE_RECORD=1` (on a machine that has the API keys) and commit the new files in `tests/cassettes`. Every other run, including CI, replays them and cannot spend a cent. A plain persistent memoizer that records on a miss is `memoize_calls(..., mode="record")`.

## Design rules

**Cache the top-level composite.** Wrap the whole `prompt -> JSON -> asset URL -> downloaded bytes` pipeline, not each API step inside it. The cache then maps one intent to one final result: fewer cassettes to review, and refactoring the internals does not invalidate them. Nothing enforces this; it is guidance.

**The key is over the semantic input.** The default key (`xdol.keys.robust_key`) is a SHA-256 of the call after normalizing superficial differences: dict key order, surrounding whitespace, tuple vs list. It is stable across runs and machines, and it raises `UnstableKeyError` for an argument that has no stable description (an object whose `repr` is a memory address, a lambda) instead of silently producing a key that changes every run. To keep something out of the key, name it in `drop=`:

```python
@memoize_calls(store=CASSETTES, drop=["logger", "timeout"])
def generate(prompt, *, logger=None, timeout=30): ...
```

Two things to know. With the default key, `f("x")` and `f(prompt="x")` are different calls (the key is over the arguments as passed). With `drop=` (or `key_func=make_key_func(f)`), calls are bound to the signature first, so they are the same call, but the keys are computed differently: **adding `drop=` to an already-recorded function changes every key**, and you re-record. Decide on `drop=` before committing cassettes. For a method, `drop=["self"]`. For a custom type, define `__cache_key__(self)` returning something JSON-able. Never key on an incidental rendering of global state (a prompt that embeds a whole registry, a timestamp, an absolute path): one registration elsewhere would invalidate every cassette. That is what the replay-only default makes loud, with a `CacheMiss` that says so.

## Codecs: text, binary, and composite results

| `codec=` | the function returns | stored as |
|---|---|---|
| `"json"` (default for a path) | JSON-able object | `<key>`, human-readable (sorted keys, diffs well) |
| `"text"` | `str` | `<key>`, UTF-8 |
| `"bytes"` | `bytes` (image, audio) | `<key>` |
| `"paired"` | `(metadata, asset_bytes)` | `<key>.json` + `<key>.bin` |

`"paired"` is for a result that mixes metadata with an asset: the pair is written asset first, metadata last (the metadata makes the entry exist), and carries a digest of the asset so a torn write from two concurrent recorders is detected rather than returned. Add your own codec with `xdol.x_codecs.CODECS["name"] = Codec(encode, decode)`.

Results are returned exactly as replay would return them, also on the recording run (so a JSON round-trip that turns a tuple into a list shows up in the first run, not only the second). `reread_after_write=False` skips that re-read (useful for very large assets) and returns the raw result on the recording run.

## The store is a `MutableMapping`

`store=` takes a directory path (an atomic, committable file store: `xdol.memoize.file_cache_store`) or any `MutableMapping`. Pass a *bytes* store plus a `codec` and the codec is applied on top (forget the `codec=` and a bytes store receives a Python object: the paid call runs, then storing fails with `CacheWriteError`, whose `.result` still holds the value), so the call site does not change when the cassettes move:

```python
import s3dol

store = s3dol.s3_store("my-ci-cassettes", prefix="myproj/")   # MutableMapping[str, bytes]

@memoize_calls(store=store, codec="json")
def plan_video(prompt): ...
```

Record locally to a folder, upload the files to the bucket (`aws s3 sync tests/cassettes s3://my-ci-cassettes/myproj/`), and replay from S3 in GitHub Actions where there is no server filesystem to commit to. The same pattern works for any store with the mapping interface. `tests/test_memoize_s3.py` shows it end to end with s3dol's in-memory fake.

A store failure (network down, access denied, an archived object) is never mistaken for a miss: it propagates, and no real call is made.

## Wiring it into pytest

```python
# conftest.py
import os

def pytest_addoption(parser):
    parser.addoption("--record-cassettes", action="store_true",
                     help="let cache misses call the real (paid) API and record the result")

def pytest_configure(config):
    if config.getoption("--record-cassettes"):
        os.environ["XDOL_MEMOIZE_RECORD"] = "1"      # read at call time, so this is early enough
```

Then `pytest` replays, and `pytest --record-cassettes` records the misses. In the test module, wrap the real function once (`live_plan = memoize_calls(real_plan, store=CASSETTES, drop=[...])`) and call `live_plan` from the tests.

## Modes and other knobs

| `mode=` | hit | miss |
|---|---|---|
| `"replay"` (default, when the env opt-in is off) | replays | raises `CacheMiss`, calls nothing |
| `"record"` | replays | calls, stores |
| `"refresh"` | calls, re-stores | calls, stores |
| `"off"` | calls, store untouched | calls, store untouched |

`mode` may also be a zero-argument callable, evaluated on every call. `default_mode` changes what the environment-driven default means; `record_env` renames the variable. An entry that cannot be decoded raises `CacheCorrupt` in replay and is recomputed (with a warning) when recording. A fresh result that cannot be stored (not JSON-able, say) raises `CacheWriteError` carrying the result, so the paid-for value is not lost. Async functions are supported; exceptions are never cached.

The wrapper keeps the function's signature and metadata (`functools.wraps`) and exposes `.cache_store`, `.key_func` (also `.cache_key(*args, **kwargs)`: the key a call would use) and `.cache_mode()`. Other keyword arguments: `on_write_error="warn"` returns the result with a warning instead of raising `CacheWriteError`; `copy_results` deep-copies values read from an object-valued mapping store (default on for those) so callers cannot mutate the cache; `corrupt_errors` names the exceptions that mean "undecodable entry" for an object-valued store.

## Limits

- Concurrent recorders of the same key may both call the function; the last write wins (the paired codec detects a torn pair).
- Generators and coroutines returned by a sync function cannot be cached; decorate the async function itself.
- Keys are a stability contract: committed cassettes depend on them, so `robust_key` does not change for JSON-able inputs between releases.
