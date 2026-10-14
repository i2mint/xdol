"""Robust, stable cache keys: turn a call's inputs into a short, filesystem-safe string.

A persistent cache (see :mod:`xdol.memoize`) is only as good as its keys. A key must be

- **stable across runs and machines** -- never derived from a memory address, set
  iteration order or dict insertion order;
- **insensitive to superficial differences** -- dict key order, surrounding whitespace;
- **loud about what it cannot make stable** -- an object whose only description is
  ``<Foo object at 0x7f...>`` would give a *different key every run* (a cache that
  never hits, or, in replay-only mode, a spurious miss), so it raises
  :class:`UnstableKeyError` instead of silently hashing the address.

>>> robust_key({"b": 1, "a": 2}, prompt="  hi ") == robust_key({"a": 2, "b": 1}, prompt="hi")
True
>>> len(robust_key(1, 2, x=3))
32

Keys are a **stability contract**: committed caches (e.g. cassette directories) are keyed
by these hashes, so the output for JSON-able inputs does not change between releases.

Customize with :func:`make_key_func` -- drop non-semantic arguments (callbacks, loggers),
bind to the wrapped function's signature, pick the digest, or teach it your own types
with a ``__cache_key__`` method.

>>> def f(prompt, *, logger=None, retries=3): ...
>>> key = make_key_func(f, drop=["logger", "retries"])
>>> key("hi", logger=print) == key(prompt="hi", retries=9)
True
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import hashlib
import inspect
import json
import pathlib
import re
import warnings
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Literal

DFLT_DIGEST = "sha256"
DFLT_KEY_LENGTH = 32
DFLT_ON_UNSTABLE = "raise"

UnstablePolicy = Literal["raise", "warn", "ignore"]

#: What Python's default ``object.__repr__`` looks like: tied to a memory address.
_ADDRESS_REPR = re.compile(r" at 0x[0-9a-fA-F]+|<\w*Mock id='\d+'>")


class UnstableKeyError(TypeError):
    """An input has no run-stable description, so no stable key can be made from it.

    Fix it by giving the object a ``__cache_key__()`` method, dropping the argument from
    the key (``make_key_func(f, drop=[...])``), or passing ``on_unstable="ignore"`` if
    you accept a key that may change between runs.
    """


def _digest_hex(data: bytes, digest: str = DFLT_DIGEST) -> str:
    return hashlib.new(digest, data).hexdigest()


def _unstable(obj: Any, on_unstable: UnstablePolicy) -> str:
    """The ``repr`` of ``obj``, vetted for run-stability according to the policy."""
    text = repr(obj)
    if _ADDRESS_REPR.search(text) and on_unstable != "ignore":
        msg = (
            f"Cannot build a stable cache key from {type(obj).__qualname__!r} instance "
            f"(its repr contains a memory address, which changes every run). Give it a "
            f"__cache_key__() method, or drop that argument from the key."
        )
        if on_unstable == "raise":
            raise UnstableKeyError(msg)
        warnings.warn(msg, stacklevel=2)
    return text


def normalize(
    obj: Any,
    *,
    collapse_whitespace: bool = False,
    on_unstable: UnstablePolicy = DFLT_ON_UNSTABLE,
) -> Any:
    """Recursively reduce ``obj`` to a JSON-able structure that is equal for equal meaning.

    - strings are stripped (and, with ``collapse_whitespace``, inner runs of whitespace
      become one space -- off by default since whitespace can be meaningful in code or
      markdown);
    - mappings get string keys, sorted; lists and tuples become lists;
    - sets are sorted; bytes are replaced by their digest; paths, enums, dates,
      dataclasses, numpy-like arrays and callables get a tagged, stable description;
    - an object with a ``__cache_key__()`` method is described by what it returns;
    - pandas objects are described by their full content; bound methods by function and
      instance; lambdas raise (indistinguishable);
    - anything else falls back to ``repr``, vetted by ``on_unstable`` (see
      :class:`UnstableKeyError`). A ``repr`` can still be lossy (truncated, or leaving out
      state): for such types define ``__cache_key__``.

    >>> normalize({"b": " x ", "a": (1, 2)})
    {'a': [1, 2], 'b': 'x'}
    >>> normalize({3, 1, 2})
    {'__set__': [1, 2, 3]}
    """
    rec = lambda o: normalize(  # noqa: E731
        o, collapse_whitespace=collapse_whitespace, on_unstable=on_unstable
    )
    # Scalars (including IntEnum / str-Enum members, as plain values: they compare equal
    # to them, and this is what the prototype's keys did).
    if obj is None or isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return int(obj)
    if isinstance(obj, float):
        return float(obj)
    if isinstance(obj, str):
        obj = str.strip(obj)
        return re.sub(r"\s+", " ", obj) if collapse_whitespace else obj
    if hasattr(obj, "__cache_key__") and not isinstance(obj, type):
        return rec(obj.__cache_key__())
    if isinstance(obj, enum.Enum):
        cls = type(obj)
        return {"__enum__": f"{cls.__module__}.{cls.__qualname__}.{obj.name}"}
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return {"__bytes__": _digest_hex(bytes(obj))}
    if isinstance(obj, Mapping):
        return _normalize_mapping(obj, rec)
    if isinstance(obj, (list, tuple)):
        return [rec(x) for x in obj]
    if isinstance(obj, (set, frozenset)):
        items = [rec(x) for x in obj]
        return {"__set__": sorted(items, key=lambda x: json.dumps(x, sort_keys=True))}
    if isinstance(obj, pathlib.PurePath):
        return {"__path__": obj.as_posix()}
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return {"__datetime__": obj.isoformat()}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        fields = {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
        return {"__dataclass__": type(obj).__qualname__, "fields": rec(fields)}
    root = type(obj).__module__.split(".")[0]
    if root == "pandas" and (hasattr(obj, "index") or hasattr(obj, "dtype")):
        return _normalize_pandas(obj, rec)
    if root == "numpy" and hasattr(obj, "tolist") and hasattr(obj, "dtype"):
        return _normalize_numpy(obj, rec)
    # Looked up on the type, so duck-typed mocks (which invent attributes) don't match.
    if callable(getattr(type(obj), "model_dump", None)):  # pydantic v2
        return {"__model__": type(obj).__qualname__, "fields": rec(obj.model_dump())}
    if inspect.isroutine(obj) and _is_bound(obj):
        # A bound method (Python or builtin) is its function *and* its instance.
        return {
            "__method__": _callable_name(getattr(obj, "__func__", obj)),
            "self": rec(obj.__self__),
        }
    if inspect.isroutine(obj) or inspect.isclass(obj):
        name = _callable_name(obj)
        if name.endswith("<lambda>"):
            _unstable_name(name, on_unstable)
        return {"__callable__": name}
    return _unstable(obj, on_unstable)


def _normalize_pandas(obj: Any, rec: Callable) -> dict:
    """Content digest (lossless, unlike ``to_json``), plus dtypes, columns and shape."""
    import pandas as pd

    try:
        digest = _digest_hex(
            pd.util.hash_pandas_object(obj, index=True).values.tobytes()
        )
    except TypeError as e:  # unhashable cell values
        raise UnstableKeyError(f"Cannot hash pandas content: {e}") from e
    dtypes = (
        obj.dtypes.astype(str).to_dict() if hasattr(obj, "columns") else str(obj.dtype)
    )
    return {
        "__pandas__": type(obj).__qualname__,
        "digest": digest,
        "shape": list(obj.shape),
        "dtypes": rec(dtypes),
        "columns": rec(list(map(str, getattr(obj, "columns", [])))),
        "names": rec([str(n) for n in getattr(obj.index, "names", [])]),
    }


def _normalize_numpy(obj: Any, rec: Callable) -> dict:
    """Arrays by dtype, shape and raw bytes digest (fast); object arrays element-wise."""
    if obj.dtype.kind == "O":  # bytes would be pointers
        return {"__array__": "object", "value": rec(obj.tolist())}
    import numpy as np

    raw = np.ascontiguousarray(obj).tobytes()
    return {
        "__array__": str(obj.dtype),
        "shape": list(getattr(obj, "shape", ())),
        "digest": _digest_hex(raw),
    }


def _is_bound(routine: Any) -> bool:
    owner = getattr(routine, "__self__", None)
    return owner is not None and not inspect.ismodule(owner)


def _callable_name(obj: Any) -> str:
    return f"{getattr(obj, '__module__', '')}:{getattr(obj, '__qualname__', repr(obj))}"


def _unstable_name(name: str, on_unstable: UnstablePolicy) -> None:
    if on_unstable == "ignore":
        return
    msg = (
        f"Cannot build a stable cache key from {name}: lambdas are indistinguishable "
        f"by name. Use a named function, or drop that argument from the key."
    )
    if on_unstable == "raise":
        raise UnstableKeyError(msg)
    warnings.warn(msg, stacklevel=3)


def _normalize_mapping(obj: Mapping, rec: Callable) -> dict:
    """Mapping with string keys, sorted. Distinct keys must stay distinct.

    ``str``/``int``/``float``/``bool``/``None`` keys become their ``str`` (as JSON would);
    other hashables (tuples, frozensets, ...) become the JSON of their normal form. Two
    keys that end up the same string (``1`` and ``"1"``) would silently drop a value and
    make equal dicts hash differently, so that raises :class:`UnstableKeyError`.
    """

    def key_str(k: Any) -> str:
        if isinstance(k, (str, int, float, bool)) or k is None:
            return str(k)
        return json.dumps(rec(k), sort_keys=True, ensure_ascii=False)

    keyed = [(key_str(k), k) for k in obj]
    names = [name for name, _ in keyed]
    if len(set(names)) != len(names):
        raise UnstableKeyError(
            f"Mapping keys collide once made strings (e.g. 1 and '1'): {sorted(names)}"
        )
    return {name: rec(obj[k]) for name, k in sorted(keyed, key=lambda nk: nk[0])}


def hash_payload(
    payload: Any,
    *,
    digest: str = DFLT_DIGEST,
    length: int = DFLT_KEY_LENGTH,
) -> str:
    """Hash an already-normalized JSON-able ``payload`` to a short hex string."""
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=repr)
    return _digest_hex(blob.encode("utf-8"), digest)[:length]


def robust_key(*args: Any, **kwargs: Any) -> str:
    """The default key function: a stable SHA-256 (32 hex chars) of the normalized call.

    Calls whose arguments are semantically equal -- same values up to dict key order and
    surrounding whitespace -- get the same key.

    >>> robust_key("a", n=1) == robust_key("a ", n=1)
    True
    >>> robust_key("a", n=1) == robust_key("a", n=2)
    False
    """
    payload = {"args": normalize(list(args)), "kwargs": normalize(kwargs)}
    return hash_payload(payload)


def make_key_func(
    func: Callable | None = None,
    *,
    drop: Iterable[str] = (),
    apply_defaults: bool = False,
    collapse_whitespace: bool = False,
    on_unstable: UnstablePolicy = DFLT_ON_UNSTABLE,
    digest: str = DFLT_DIGEST,
    length: int = DFLT_KEY_LENGTH,
    prefix: str = "",
) -> Callable[..., str]:
    """Build a key function ``(*args, **kwargs) -> str``, customized.

    Args:
        func: if given, calls are bound to its signature first, so ``f(1, 2)``,
            ``f(1, b=2)`` and ``f(a=1, b=2)`` share a key. Required for ``drop``.
        drop: names of parameters of ``func`` to leave out of the key -- the
            non-semantic ones (callbacks, loggers, timeouts, ``self``).
        apply_defaults: with ``func``, also hash default values, so ``f(x)`` equals
            ``f(x, y=<default>)``. Off by default: adding a defaulted parameter later
            then does not invalidate existing caches (changing a default does not
            either -- turn this on if that matters to you).
        collapse_whitespace, on_unstable: see :func:`normalize`.
        digest, length: hash algorithm (any :mod:`hashlib` name) and key length in hex chars.
        prefix: prepended to every key (e.g. a namespace or version: ``"v2-"``).

    With no arguments this is :func:`robust_key`.

    >>> make_key_func()("x") == robust_key("x")
    True
    >>> def f(a, b=1, *, log=None): ...
    >>> k = make_key_func(f, drop=["log"], apply_defaults=True)
    >>> k(1) == k(1, 1) == k(a=1, log=print)
    True
    """
    drop = frozenset(drop)
    if drop and func is None:
        raise ValueError("`drop` needs `func`: it names parameters of the function.")
    sig = inspect.signature(func) if func is not None else None
    var_kw = None
    if sig is not None:
        var_kw = next(
            (n for n, p in sig.parameters.items() if p.kind is p.VAR_KEYWORD), None
        )
    if sig is not None and var_kw is None and (unknown := drop - set(sig.parameters)):
        raise ValueError(f"`drop` names no parameter of {func!r}: {sorted(unknown)}")
    norm = lambda o: normalize(  # noqa: E731
        o, collapse_whitespace=collapse_whitespace, on_unstable=on_unstable
    )

    def key_func(*args: Any, **kwargs: Any) -> str:
        if sig is None:
            payload = {"args": norm(list(args)), "kwargs": norm(kwargs)}
        else:
            bound = sig.bind(*args, **kwargs)
            if apply_defaults:
                bound.apply_defaults()
            arguments = {k: v for k, v in bound.arguments.items() if k not in drop}
            if var_kw in arguments:  # `drop` may also name entries of **kwargs
                arguments[var_kw] = {
                    k: v for k, v in arguments[var_kw].items() if k not in drop
                }
            payload = {"bound": norm(arguments)}
        return prefix + hash_payload(payload, digest=digest, length=length)

    return key_func
