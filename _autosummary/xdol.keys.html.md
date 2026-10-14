# xdol.keys

Robust, stable cache keys: turn a call’s inputs into a short, filesystem-safe string.

A persistent cache (see `xdol.memoize`) is only as good as its keys. A key must be

- **stable across runs and machines** – never derived from a memory address, set
  iteration order or dict insertion order;
- **insensitive to superficial differences** – dict key order, surrounding whitespace;
- **loud about what it cannot make stable** – an object whose only description is
  `<Foo object at 0x7f...>` would give a *different key every run* (a cache that
  never hits, or, in replay-only mode, a spurious miss), so it raises
  [`UnstableKeyError`](#xdol.keys.UnstableKeyError) instead of silently hashing the address.

```pycon
>>> robust_key({"b": 1, "a": 2}, prompt="  hi ") == robust_key({"a": 2, "b": 1}, prompt="hi")
True
>>> len(robust_key(1, 2, x=3))
32
```

Keys are a **stability contract**: committed caches (e.g. cassette directories) are keyed
by these hashes, so the output for JSON-able inputs does not change between releases.

Customize with [`make_key_func()`](#xdol.keys.make_key_func) – drop non-semantic arguments (callbacks, loggers),
bind to the wrapped function’s signature, pick the digest, or teach it your own types
with a `__cache_key__` method.

```pycon
>>> def f(prompt, *, logger=None, retries=3): ...
>>> key = make_key_func(f, drop=["logger", "retries"])
>>> key("hi", logger=print) == key(prompt="hi", retries=9)
True
```

### Functions

| [`hash_payload`](#xdol.keys.hash_payload)(payload, \*[, digest, length])      | Hash an already-normalized JSON-able `payload` to a short hex string.              |
|---------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| [`make_key_func`](#xdol.keys.make_key_func)([func, drop, apply_defaults, ...]) | Build a key function `(*args, **kwargs) -> str`, customized.                       |
| [`normalize`](#xdol.keys.normalize)(obj, \*[, collapse_whitespace, ...])   | Recursively reduce `obj` to a JSON-able structure that is equal for equal meaning. |
| [`robust_key`](#xdol.keys.robust_key)(\*args, \*\*kwargs)                   | The default key function: a stable SHA-256 (32 hex chars) of the normalized call.  |

### Exceptions

| [`UnstableKeyError`](#xdol.keys.UnstableKeyError)   | An input has no run-stable description, so no stable key can be made from it.   |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------|

### *exception* xdol.keys.UnstableKeyError

Bases: [`TypeError`](https://docs.python.org/3/builtins/exceptions.html#TypeError)

An input has no run-stable description, so no stable key can be made from it.

Fix it by giving the object a `__cache_key__()` method, dropping the argument from
the key (`make_key_func(f, drop=[...])`), or passing `on_unstable="ignore"` if
you accept a key that may change between runs.

### xdol.keys.hash_payload(payload, , digest='sha256', length=32)

Hash an already-normalized JSON-able `payload` to a short hex string.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

### xdol.keys.make_key_func(func=None, , drop=(), apply_defaults=False, collapse_whitespace=False, on_unstable='raise', digest='sha256', length=32, prefix='')

Build a key function `(*args, **kwargs) -> str`, customized.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – if given, calls are bound to its signature first, so `f(1, 2)`,
    `f(1, b=2)` and `f(a=1, b=2)` share a key. Required for `drop`.
  * **drop** ([`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – names of parameters of `func` to leave out of the key – the
    non-semantic ones (callbacks, loggers, timeouts, `self`).
  * **apply_defaults** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – with `func`, also hash default values, so `f(x)` equals
    `f(x, y=<default>)`. Off by default: adding a defaulted parameter later
    then does not invalidate existing caches (changing a default does not
    either – turn this on if that matters to you).
  * **collapse_whitespace** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – see [`normalize()`](#xdol.keys.normalize).
  * **on_unstable** ([`Literal`](https://docs.python.org/3/library/typing.html#typing.Literal)[`'raise'`, `'warn'`, `'ignore'`]) – see [`normalize()`](#xdol.keys.normalize).
  * **digest** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – hash algorithm (any [`hashlib`](https://docs.python.org/3/library/hashlib.html#module-hashlib) name) and key length in hex chars.
  * **length** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – hash algorithm (any [`hashlib`](https://docs.python.org/3/library/hashlib.html#module-hashlib) name) and key length in hex chars.
  * **prefix** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – prepended to every key (e.g. a namespace or version: `"v2-"`).
* **Return type:**
  [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`...`](https://docs.python.org/3/builtins/constants.html#Ellipsis), [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

With no arguments this is [`robust_key()`](#xdol.keys.robust_key).

```pycon
>>> make_key_func()("x") == robust_key("x")
True
>>> def f(a, b=1, *, log=None): ...
>>> k = make_key_func(f, drop=["log"], apply_defaults=True)
>>> k(1) == k(1, 1) == k(a=1, log=print)
True
```

### xdol.keys.normalize(obj, , collapse_whitespace=False, on_unstable='raise')

Recursively reduce `obj` to a JSON-able structure that is equal for equal meaning.

- strings are stripped (and, with `collapse_whitespace`, inner runs of whitespace
  become one space – off by default since whitespace can be meaningful in code or
  markdown);
- mappings get string keys, sorted; lists and tuples become lists;
- sets are sorted; bytes are replaced by their digest; paths, enums, dates,
  dataclasses, numpy-like arrays and callables get a tagged, stable description;
- an object with a `__cache_key__()` method is described by what it returns;
- pandas objects are described by their full content; bound methods by function and
  instance; lambdas raise (indistinguishable);
- anything else falls back to `repr`, vetted by `on_unstable` (see
  [`UnstableKeyError`](#xdol.keys.UnstableKeyError)). A `repr` can still be lossy (truncated, or leaving out
  > state): for such types define `__cache_key__`.

```pycon
>>> normalize({"b": " x ", "a": (1, 2)})
{'a': [1, 2], 'b': 'x'}
>>> normalize({3, 1, 2})
{'__set__': [1, 2, 3]}
```

* **Return type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)

### xdol.keys.robust_key(\*args, \*\*kwargs)

The default key function: a stable SHA-256 (32 hex chars) of the normalized call.

Calls whose arguments are semantically equal – same values up to dict key order and
surrounding whitespace – get the same key.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

```pycon
>>> robust_key("a", n=1) == robust_key("a ", n=1)
True
>>> robust_key("a", n=1) == robust_key("a", n=2)
False
```
