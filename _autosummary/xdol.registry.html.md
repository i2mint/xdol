# xdol.registry

A typed dict-backed plugin registry.

The [`Registry`](#xdol.registry.Registry) is the dict-pattern that recurs across packages
(`falaw.registry`, `lacing.processors`, `lookbook.registry`, …): a
dict, a `register(key, value)` call, a `get(key)` lookup, an iteration
protocol, and a small number of conveniences that each package keeps
re-implementing — aliases, lazy loading, decorator-form registration, and
conflict policies. `Registry` collects these into one builtin-only,
`MutableMapping`-compatible class.

Basic use:

```pycon
>>> from xdol.registry import Registry
>>> tools = Registry[str](name="tools")
>>> tools.register("hello", "say hi")
'say hi'
>>> tools["hello"]
'say hi'
>>> sorted(tools)
['hello']
```

Decorator form:

```pycon
>>> handlers = Registry()
>>> @handlers.register_decorator("greet")
... def greet(name): return f"hi {name}"
>>> handlers["greet"]("ada")
'hi ada'
```

Aliases:

```pycon
>>> models = Registry()
>>> models.register("fal-ai/flux/dev", {"family": "flux"})
{'family': 'flux'}
>>> models.alias("flux", "fal-ai/flux/dev")
>>> models["flux"]
{'family': 'flux'}
>>> "flux" in models
True
```

Conflict policies — `error` (default), `replace`, `keep`:

```pycon
>>> r = Registry(on_conflict="error")
>>> r.register("k", 1)
1
>>> r.register("k", 2)
Traceback (most recent call last):
    ...
xdol.registry.RegistryConflict: 'k' is already registered in <Registry>
```

```pycon
>>> r2 = Registry(on_conflict="keep")
>>> r2.register("k", 1)
1
>>> r2.register("k", 2)  # silently kept
1
>>> r2["k"]
1
```

Plain assignment replaces; opt in to `strict_setitem` to make it honour the policy:

```pycon
>>> loose = Registry()
>>> loose["k"] = 1
>>> loose["k"] = 2  # explicit assignment replaces
>>> loose["k"]
2
>>> strict = Registry(strict_setitem=True)
>>> strict["k"] = 1
>>> strict["k"] = 2
Traceback (most recent call last):
    ...
xdol.registry.RegistryConflict: 'k' is already registered in <Registry>
```

Lazy loading — value is built on first access:

```pycon
>>> calls = []
>>> def make_thing():
...     calls.append(1)
...     return {"built": True}
>>> r3 = Registry()
>>> r3.register_lazy("thing", make_thing)
>>> calls
[]
>>> r3["thing"]
{'built': True}
>>> r3["thing"]  # cached after first build
{'built': True}
>>> calls
[1]
```

Subscription — observe registrations as they happen:

```pycon
>>> r4 = Registry()
>>> seen = []
>>> sub = r4.subscribe(lambda key, value: seen.append((key, value)))
>>> r4.register("a", 1)
1
>>> r4.register("b", 2)
2
>>> seen
[('a', 1), ('b', 2)]
>>> sub.unsubscribe()
>>> r4.register("c", 3)
3
>>> seen  # no longer notified
[('a', 1), ('b', 2)]
```

### Classes

| [`Registry`](#xdol.registry.Registry)(\*[, name, on_conflict, strict_setitem])   | A typed dict-backed plugin registry.                                                      |
|------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| [`Subscription`](#xdol.registry.Subscription)(registry, callback)                    | Handle returned by [`Registry.subscribe()`](#xdol.registry.Registry.subscribe). |

### Exceptions

| [`RegistryConflict`](#xdol.registry.RegistryConflict)   | Raised when registering a key that is already taken under `on_conflict='error'`.                 |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| [`RegistryError`](#xdol.registry.RegistryError)      | Base for registry errors.                                                                        |
| [`RegistryMissing`](#xdol.registry.RegistryMissing)    | Raised by [`Registry.get()`](#xdol.registry.Registry.get) when a key is missing. |

### *class* xdol.registry.Registry(, name='', on_conflict='error', strict_setitem=False)

Bases: [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping), [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`V`]

A typed dict-backed plugin registry.

* **Parameters:**
  * **name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Optional name; appears in error messages and `repr`.
  * **on_conflict** ([`Literal`](https://docs.python.org/3/library/typing.html#typing.Literal)[`'error'`, `'replace'`, `'keep'`]) – `'error'` (default) raises [`RegistryConflict`](#xdol.registry.RegistryConflict) when registering
    a key that already exists. `'replace'` silently overwrites.
    `'keep'` silently keeps the original.
  * **strict_setitem** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Opt-in (default `False`). By default `reg[k] = v` is an explicit
    assignment that replaces whatever is there, bypassing `on_conflict`.
    With `strict_setitem=True` it follows the `on_conflict` policy exactly
    like [`register()`](#xdol.registry.Registry.register) (so `MutableMapping.update` and `setdefault`
    do too; note `update` is not atomic: entries before the first conflict stay).
  * **(****reading** (*Keys that are aliases resolve to their canonical key everywhere*)

:param :
:param `reg[k] = v`:
:param [`register()`](#xdol.registry.Registry.register):
:param [`register_lazy()`](#xdol.registry.Registry.register_lazy)):
:param so the `on_conflict`:
:param policy always applies to the canonical entry.:
:param Implements `MutableMapping` so anything that takes a mapping (`dict(...)`:
:param :
:param iteration:
:param length checks:
:param `in` lookups:
:param `.items()`) just works.:

#### alias(alias, target)

Make `alias` resolve to `target` (which must already be registered).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### get(key, default=<object object>)

Like `dict.get` but raises [`RegistryMissing`](#xdol.registry.RegistryMissing) if no default given.

#### keys_with_tag(tag)

Return canonical keys whose tag set contains `tag`.

* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

#### register(key, value, , tags=())

Register `value` under `key`. Returns `value` so it can be used inline.

If `key` is an alias, it is resolved to its canonical key first, so the
`on_conflict` policy applies to that entry (no unreachable shadow entry).

* **Return type:**
  [`TypeVar`](https://docs.python.org/3/library/typing.html#typing.TypeVar)(`V`)

#### register_decorator(key, , tags=())

Return a decorator that registers the decorated object under `key`.

* **Return type:**
  [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`TypeVar`](https://docs.python.org/3/library/typing.html#typing.TypeVar)(`V`)], [`TypeVar`](https://docs.python.org/3/library/typing.html#typing.TypeVar)(`V`)]

```pycon
>>> r = Registry()
>>> @r.register_decorator("hi")
... def hi(): return "hello"
>>> r["hi"]()
'hello'
```

#### register_lazy(key, loader, , tags=())

Register a no-arg `loader` callable; value is built on first `__getitem__`.

Once built, the result replaces the loader and is cached.
Alias keys resolve to their canonical key, as in [`register()`](#xdol.registry.Registry.register).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### search(, tags=())

Return values whose tag set contains ALL of `tags` (empty `tags` = all).

* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TypeVar`](https://docs.python.org/3/library/typing.html#typing.TypeVar)(`V`)]

#### subscribe(callback)

Call `callback(key, value)` on every eager registration.

Lazy registrations do NOT fire the callback at registration time;
only when the value is materialized via `__getitem__`. This
keeps subscribers free of partially-built state.

* **Return type:**
  [`Subscription`](#xdol.registry.Subscription)

### *exception* xdol.registry.RegistryConflict

Bases: [`RegistryError`](#xdol.registry.RegistryError)

Raised when registering a key that is already taken under `on_conflict='error'`.

### *exception* xdol.registry.RegistryError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base for registry errors.

### *exception* xdol.registry.RegistryMissing

Bases: [`RegistryError`](#xdol.registry.RegistryError), [`KeyError`](https://docs.python.org/3/builtins/exceptions.html#KeyError)

Raised by [`Registry.get()`](#xdol.registry.Registry.get) when a key is missing.

### *class* xdol.registry.Subscription(registry, callback)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Handle returned by [`Registry.subscribe()`](#xdol.registry.Registry.subscribe). Call `unsubscribe()` to detach.
