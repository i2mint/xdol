"""Persistent memoization with record/replay modes, for costly calls.

A transparent wrapper that persists a function's results in a ``MutableMapping`` keyed by
a stable hash of the call. It is a VCR cassette for *any* function, not just HTTP: the
first run records, every later run replays from the store -- no call, no cost, no network.
The driving use is tests of code that calls paid APIs (LLMs, image/video generation),
which must stay free and fast, but it serves any expensive, deterministic-enough call.

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

**Replay is the default; recording is an opt-in.** Under the default ``mode=None`` a call
whose key is not in the store raises :class:`CacheMiss` instead of calling the function,
unless the run sets the ``XDOL_MEMOIZE_RECORD`` environment variable (see
:func:`recording_enabled`). A key that quietly moved would otherwise read as a normal,
billable test run. Spending is an act that carries a flag. For a plain memoizer that
records on a miss, pass ``mode="record"`` (as above).

Four pieces, each a keyword argument with a good default (a *seam*):

- ``key_func`` -- input to key; default :func:`xdol.keys.robust_key` (see also ``drop=``);
- ``codec`` -- result to bytes; ``"json"`` (default), ``"text"``, ``"bytes"`` for binary
  assets, or ``"paired"`` for a ``(metadata, asset_bytes)`` result stored as
  ``<key>.json`` + ``<key>.bin`` (see :mod:`xdol.x_codecs`);
- ``store`` -- a directory path (a stable, committable folder: see
  :func:`file_cache_store`), or any ``MutableMapping``. Pass a bytes store such as
  ``s3dol.s3_store(bucket)`` and the codec is applied on top, so swapping the filesystem
  for S3 changes nothing at the call site;
- ``mode`` -- ``"replay"``, ``"record"``, ``"refresh"`` or ``"off"``; see :data:`MODES`.

**Cache the top-level composite**: wrap the whole ``prompt -> JSON -> asset URL ->
downloaded bytes`` pipeline, not each intermediate API step, so one intent maps to one
final result. Nothing enforces this; it keeps the surface area (and the number of
cassettes to review) small.

The wrapper preserves the function's signature and metadata (``functools.wraps``) and
supports coroutine functions. Exceptions raised by the function are never cached.
"""

from __future__ import annotations

import copy
import functools
import inspect
import os
import stat
import uuid
import warnings
from collections.abc import Callable, Iterable, Iterator, MutableMapping
from pathlib import Path
from typing import Any, Literal

from dol import Files

from xdol.keys import make_key_func, robust_key
from xdol.x_codecs import CodecDecodeError, get_codec, paired_store, with_codec

Mode = Literal["replay", "record", "refresh", "off"]

MODES: dict[str, str] = {
    "replay": "a hit replays; a miss raises CacheMiss and calls nothing (the safe default)",
    "record": "a hit replays; a miss calls the function and stores the result",
    "refresh": "always calls the function and (re)stores the result",
    "off": "always calls the function; the store is not touched",
}

#: Set to a truthy value to let ``mode=None`` calls record on a miss.
RECORD_ENV = "XDOL_MEMOIZE_RECORD"

#: Environment-variable spellings that count as "on".
TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})

#: Errors that mean "this stored entry cannot be decoded" (as opposed to a store/network
#: failure, which must never be mistaken for a missing entry and cost a real call).
DFLT_CORRUPT_ERRORS = (CodecDecodeError,)


class CacheMiss(LookupError):
    """A memoized call missed its store while recording was disabled.

    The alternative to this exception is a real, possibly billable call nobody asked
    for. Two things cause it: an input the store has genuinely never seen, and a cache
    **key** that moved under an unchanged input -- usually because the key was computed
    over something incidental (a rendered global registry, a timestamp, a path).
    """


class CacheCorrupt(RuntimeError):
    """A stored entry exists but cannot be decoded (replay mode refuses to guess)."""


class CacheWriteError(RuntimeError):
    """The function ran but its result could not be stored (e.g. not JSON-serializable).

    The result is not lost: it is the ``result`` attribute.
    """

    def __init__(self, message: str, result: Any):
        super().__init__(message)
        self.result = result


def recording_enabled(env: str = RECORD_ENV) -> bool:
    """True when the environment variable ``env`` is set to a truthy value.

    The opt-in is environment-level rather than per-call, so "this run may spend" is one
    visible fact about the run instead of a property of each test.

    >>> recording_enabled("XDOL_SURELY_UNSET_VARIABLE")
    False
    """
    return os.environ.get(env, "").strip().lower() in TRUTHY_ENV_VALUES


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


class AtomicFiles(Files):
    """A bytes store over a directory whose writes are atomic.

    Plain file writes can leave a truncated cassette if the process dies mid-write, which
    then fails to decode (or worse, decodes). Here each value is written to a temporary
    sibling and renamed into place (``os.replace``), and temporary files are invisible to
    iteration. Missing sub-directories of a key (``"a/b"``) are created.
    """

    _TMP_SUFFIX = ".xdol-tmp"

    def __init__(self, *args, delete_func=os.remove, **kwargs):
        # Cached results are derived data: deleting one should not leave a copy in the trash.
        super().__init__(*args, delete_func=delete_func, **kwargs)

    def _id_of_key(self, key):
        path = super()._id_of_key(key)
        root = os.path.abspath(self.rootdir)
        if os.path.commonpath([root, os.path.abspath(path)]) != root:
            raise ValueError(f"Key {key!r} points outside the store's root")
        return path

    # Only a *missing file* is a KeyError (a miss). dol.Files turns every error into a
    # KeyError, which would make an unreadable-but-present entry look like a miss.
    def __getitem__(self, key):
        try:
            return Path(self._id_of_key(key)).read_bytes()
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            raise KeyError(key) from None

    def __contains__(self, key) -> bool:
        try:
            return stat.S_ISREG(os.stat(self._id_of_key(key)).st_mode)
        except (FileNotFoundError, NotADirectoryError, ValueError):
            return False  # anything else (e.g. EACCES) is a failure, not "absent"

    def __delitem__(self, key):
        try:
            os.remove(self._id_of_key(key))
        except FileNotFoundError:
            raise KeyError(key) from None

    def __setitem__(self, key, value):
        path = Path(self._id_of_key(key))
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex[:8]}{self._TMP_SUFFIX}")
        try:
            tmp.write_bytes(value)
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def __iter__(self) -> Iterator[str]:
        return (k for k in super().__iter__() if not k.endswith(self._TMP_SUFFIX))


def file_cache_store(
    rootdir: str | os.PathLike, *, codec="json"
) -> MutableMapping:
    """A persistent, atomic, codec-aware ``MutableMapping`` over a directory.

    Keys are filenames, values are decoded objects. ``rootdir`` is
    created if missing and is meant to be a stable folder you commit (a cassette
    directory), not a temp dir. ``codec="paired"`` stores ``(metadata, asset_bytes)``
    values as ``<key>.json`` + ``<key>.bin``.

    >>> import tempfile
    >>> store = file_cache_store(tempfile.mkdtemp())
    >>> store["k"] = {"a": 1}
    >>> store["k"], list(store)
    ({'a': 1}, ['k'])
    """
    rootdir = os.path.abspath(rootdir)  # not relative to whatever the cwd is later
    Path(rootdir).mkdir(parents=True, exist_ok=True)
    return _apply_codec(AtomicFiles(rootdir), codec)


def _apply_codec(raw_store: MutableMapping, codec) -> MutableMapping:
    if codec == "paired":
        return paired_store(raw_store)
    return with_codec(raw_store, get_codec(codec))


def _resolve_store(store, codec) -> MutableMapping:
    """A path becomes a file store; a bytes store gets the codec; else used as is."""
    if isinstance(store, (str, os.PathLike)):
        return file_cache_store(store, codec=codec or "json")
    if codec is None:
        return store
    return _apply_codec(store, codec)


def _copy(value: Any) -> Any:
    """Deep copy, or the value itself if it cannot be copied (locks, handles, ...)."""
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _is_miss(store: MutableMapping, key: str, error: KeyError) -> bool:
    """Was this ``KeyError`` a plain absent key, rather than a failure shaped like one?

    A plain ``KeyError`` is the ``Mapping`` contract for "absent" and is trusted (the
    stores built here only raise it for a missing entry). Some stores raise ``KeyError``
    *subclasses* for failures (an archived object, a missing bucket); for those a real
    miss means the store also says the key is not there.
    """
    if type(error) is KeyError:
        return True
    return key not in store


# ---------------------------------------------------------------------------
# The memoizer
# ---------------------------------------------------------------------------


def _name(func: Callable) -> str:
    return getattr(func, "__qualname__", None) or repr(func)


def _resolve_mode(mode, default_mode: str, record_env: str) -> str:
    if callable(mode):
        mode = mode()
    if mode is None:
        mode = "record" if recording_enabled(record_env) else default_mode
    if mode not in MODES:
        raise ValueError(f"Unknown mode {mode!r}; choose from {sorted(MODES)}")
    return mode


def _miss_message(func: Callable, key: str, store: MutableMapping, env: str) -> str:
    where = getattr(store, "rootdir", None) or type(store).__name__
    return (
        f"No cached result for {_name(func)!r} at key {key!r} "
        f"in {where}. Nothing was called: this run is replay-only, so a miss fails "
        f"instead of spending.\n"
        "Either the input is genuinely new, or the cache key moved under an unchanged "
        "input (check that key_func leaves out anything incidental: global state, "
        "timestamps, absolute paths -- see `drop=`).\n"
        f"To record it, re-run with {env}=1 (or pass mode='record') where the function "
        "can really run, and commit the new entries."
    )


def memoize_calls(
    func: Callable | None = None,
    *,
    store,
    codec=None,
    key_func: Callable[..., str] | None = None,
    drop: Iterable[str] = (),
    mode: "Mode | Callable[[], Mode] | None" = None,
    default_mode: Mode = "replay",
    record_env: str = RECORD_ENV,
    corrupt_errors: tuple[type[BaseException], ...] = DFLT_CORRUPT_ERRORS,
    on_write_error: Literal["raise", "warn"] = "raise",
    copy_results: bool | None = None,
    reread_after_write: bool = True,
) -> Callable:
    """Persistently memoize ``func`` into ``store`` (record/replay).

    Usable as ``memoize_calls(fn, store=...)`` or as a decorator
    ``@memoize_calls(store=...)``.

    Args:
        func: the (costly) function; wrap the *composite* top-level call.
        store: a directory path (-> :func:`file_cache_store`), or a ``MutableMapping``.
        codec: ``None`` (default) means a path gets ``"json"`` and a mapping is used as
            it is (its values are the results). Otherwise ``store`` is a *bytes* mapping
            and this is applied on top: ``"json"``, ``"text"``, ``"bytes"``,
            ``"paired"``, a :class:`xdol.x_codecs.Codec`, or an ``(encode, decode)`` pair.
        key_func: input to key. Default :func:`xdol.keys.robust_key`.
        drop: parameter names of ``func`` to leave out of the key (callbacks, loggers,
            ``self``); builds the key with :func:`xdol.keys.make_key_func`, which also
            binds calls to the signature. Not combinable with ``key_func``.
        mode: one of :data:`MODES`, a zero-argument callable returning one (evaluated on
            every call), or ``None`` (default) for: ``"record"`` if the ``record_env``
            variable is truthy, else ``default_mode``. Read at call time, so setting the
            variable after import (e.g. in a pytest hook) works.
        default_mode: what ``mode=None`` means when the environment does not opt in.
            ``"replay"`` (default) makes spending an explicit act.
        record_env: name of the opt-in environment variable.
        corrupt_errors: exception types raised by reading an entry that mean "undecodable"
            (default: :class:`xdol.x_codecs.CodecDecodeError`, which the codecs raise;
            add your own for an object-valued store that can fail to load). In replay
            mode they raise :class:`CacheCorrupt`; when recording, the entry is recomputed
            and overwritten, with a warning. Any other error -- a network failure, a
            misconfigured store -- propagates and never costs a real call.
        copy_results: deep-copy values read from the store, so callers cannot mutate the
            cache. ``None`` (default) means: yes when ``store`` is an object-valued
            mapping used as is (a codec makes a fresh object per read anyway).
        reread_after_write: after recording, return the value as read back from the store,
            so a recording run and a replay run return *the same thing* (JSON turns tuples
            into lists and int keys into strings). Turn off to skip the extra read of a
            large asset.
        on_write_error: what to do if a fresh result cannot be stored: ``"raise"``
            :class:`CacheWriteError` (carrying the result) or ``"warn"`` and return it.

    The returned wrapper has attributes ``cache_store``, ``key_func``, ``cache_key(*args,
    **kwargs)`` and ``cache_mode()`` (the mode that would apply now).
    """
    if key_func is not None and drop:
        raise ValueError("Pass either `key_func` or `drop`, not both.")
    if on_write_error not in ("raise", "warn"):
        raise ValueError("on_write_error must be 'raise' or 'warn'")
    cache = _resolve_store(store, codec)
    if copy_results is None:
        copy_results = codec is None and not isinstance(store, (str, os.PathLike))

    def decorator(f: Callable) -> Callable:
        kf = key_func or (make_key_func(f, drop=drop) if drop else robust_key)

        def lookup(key: str, mode_now: str):
            """(hit, value). Raises on replay-mode corruption."""
            try:
                value = cache[key]
                return True, _copy(value) if copy_results else value
            except KeyError as e:
                if not _is_miss(cache, key, e):
                    raise
                return False, None
            except corrupt_errors as e:
                if mode_now == "replay":
                    raise CacheCorrupt(
                        f"Cached entry {key!r} for {_name(f)!r} cannot be decoded "
                        f"({type(e).__name__}: {e}). Delete it and re-record."
                    ) from e
                warnings.warn(
                    f"Cached entry {key!r} is undecodable ({e}); recomputing.",
                    stacklevel=3,
                )
                return False, None

        def record(key: str, result):
            if inspect.isawaitable(result) or inspect.isgenerator(result):
                raise TypeError(
                    f"{_name(f)!r} returned {type(result).__name__}, which cannot be "
                    "cached. Memoize the async function itself (not a sync wrapper "
                    "returning a coroutine), and return a concrete value."
                )
            try:
                cache[key] = _copy(result) if copy_results else result
            except Exception as e:
                msg = (
                    f"{_name(f)!r} ran but its result could not be stored under "
                    f"{key!r} ({type(e).__name__}: {e}); the result is on `.result`."
                )
                if on_write_error == "raise":
                    raise CacheWriteError(msg, result) from e
                warnings.warn(msg, stacklevel=3)
                return result
            if reread_after_write:
                try:
                    return lookup_stored(key)
                except Exception as e:  # the write succeeded; don't lose the result
                    warnings.warn(f"Could not re-read {key!r} after writing: {e}")
            return result

        def lookup_stored(key: str):
            value = cache[key]
            return _copy(value) if copy_results else value

        def plan(args, kwargs):
            """-> (mode, key, replayed?, value)"""
            mode_now = _resolve_mode(mode, default_mode, record_env)
            if mode_now == "off":
                return mode_now, None, False, None
            key = kf(*args, **kwargs)
            if mode_now == "refresh":
                return mode_now, key, False, None
            hit, value = lookup(key, mode_now)
            if not hit and mode_now == "replay":
                raise CacheMiss(_miss_message(f, key, cache, record_env))
            return mode_now, key, hit, value

        if inspect.iscoroutinefunction(f):

            @functools.wraps(f)
            async def wrapper(*args, **kwargs):
                mode_now, key, hit, value = plan(args, kwargs)
                if hit:
                    return value
                result = await f(*args, **kwargs)
                return result if mode_now == "off" else record(key, result)

        else:

            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                mode_now, key, hit, value = plan(args, kwargs)
                if hit:
                    return value
                result = f(*args, **kwargs)
                return result if mode_now == "off" else record(key, result)

        wrapper.cache_store = cache
        wrapper.key_func = kf
        wrapper.cache_key = kf
        wrapper.cache_mode = lambda: _resolve_mode(mode, default_mode, record_env)
        return wrapper

    return decorator if func is None else decorator(func)
