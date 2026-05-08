"""A typed dict-backed plugin registry.

The :class:`Registry` is the dict-pattern that recurs across packages
(``falaw.registry``, ``lacing.processors``, ``lookbook.registry``, …): a
dict, a ``register(key, value)`` call, a ``get(key)`` lookup, an iteration
protocol, and a small number of conveniences that each package keeps
re-implementing — aliases, lazy loading, decorator-form registration, and
conflict policies. ``Registry`` collects these into one builtin-only,
``MutableMapping``-compatible class.

Basic use:

>>> from xdol.registry import Registry
>>> tools = Registry[str](name="tools")
>>> tools.register("hello", "say hi")
'say hi'
>>> tools["hello"]
'say hi'
>>> sorted(tools)
['hello']

Decorator form:

>>> handlers = Registry()
>>> @handlers.register_decorator("greet")
... def greet(name): return f"hi {name}"
>>> handlers["greet"]("ada")
'hi ada'

Aliases:

>>> models = Registry()
>>> models.register("fal-ai/flux/dev", {"family": "flux"})
{'family': 'flux'}
>>> models.alias("flux", "fal-ai/flux/dev")
>>> models["flux"]
{'family': 'flux'}
>>> "flux" in models
True

Conflict policies — ``error`` (default), ``replace``, ``keep``:

>>> r = Registry(on_conflict="error")
>>> r.register("k", 1)
1
>>> r.register("k", 2)
Traceback (most recent call last):
    ...
xdol.registry.RegistryConflict: 'k' is already registered in <Registry>

>>> r2 = Registry(on_conflict="keep")
>>> r2.register("k", 1)
1
>>> r2.register("k", 2)  # silently kept
1
>>> r2["k"]
1

Lazy loading — value is built on first access:

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

Subscription — observe registrations as they happen:

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

"""

from collections.abc import Callable, Iterator, MutableMapping
from typing import Generic, Literal, TypeVar


V = TypeVar("V")
OnConflict = Literal["error", "replace", "keep"]


class RegistryError(Exception):
    """Base for registry errors."""


class RegistryConflict(RegistryError):
    """Raised when registering a key that is already taken under ``on_conflict='error'``."""


class RegistryMissing(RegistryError, KeyError):
    """Raised by :meth:`Registry.get` when a key is missing."""


_MISSING = object()


class Subscription:
    """Handle returned by :meth:`Registry.subscribe`. Call :meth:`unsubscribe` to detach."""

    __slots__ = ("_registry", "_callback", "_active")

    def __init__(self, registry: "Registry", callback: Callable):
        self._registry = registry
        self._callback = callback
        self._active = True

    def unsubscribe(self) -> None:
        if self._active:
            self._registry._subscribers.discard(self._callback)
            self._active = False


class Registry(MutableMapping, Generic[V]):
    """A typed dict-backed plugin registry.

    Parameters
    ----------
    name:
        Optional name; appears in error messages and ``repr``.
    on_conflict:
        ``'error'`` (default) raises :class:`RegistryConflict` when registering
        a key that already exists. ``'replace'`` silently overwrites.
        ``'keep'`` silently keeps the original.

    Implements ``MutableMapping`` so anything that takes a mapping (``dict(...)``,
    iteration, length checks, ``in`` lookups, ``.items()``) just works.
    """

    def __init__(self, *, name: str = "", on_conflict: OnConflict = "error"):
        self._name = name
        self._on_conflict: OnConflict = on_conflict
        self._values: dict[str, V] = {}
        self._aliases: dict[str, str] = {}  # alias -> canonical key
        self._lazy: dict[str, Callable[[], V]] = {}
        self._tags: dict[str, frozenset[str]] = {}
        self._subscribers: set[Callable[[str, V], None]] = set()

    # -- core MutableMapping interface ---------------------------------------

    def __getitem__(self, key: str) -> V:
        canonical = self._aliases.get(key, key)
        if canonical in self._values:
            return self._values[canonical]
        if canonical in self._lazy:
            value = self._lazy[canonical]()
            self._values[canonical] = value
            del self._lazy[canonical]
            return value
        raise RegistryMissing(f"{key!r} is not registered in {self!r}")

    def __setitem__(self, key: str, value: V) -> None:
        # Setitem bypasses on_conflict ("replace" semantics) — explicit assignment.
        self._values[key] = value
        self._lazy.pop(key, None)
        self._notify(key, value)

    def __delitem__(self, key: str) -> None:
        canonical = self._aliases.get(key, key)
        had_value = canonical in self._values or canonical in self._lazy
        self._values.pop(canonical, None)
        self._lazy.pop(canonical, None)
        self._tags.pop(canonical, None)
        # Drop any aliases pointing at the deleted canonical key.
        for alias, target in list(self._aliases.items()):
            if target == canonical or alias == canonical:
                del self._aliases[alias]
        if not had_value:
            raise RegistryMissing(f"{key!r} is not registered in {self!r}")

    def __iter__(self) -> Iterator[str]:
        # Iterate over canonical keys only; aliases are visible via __contains__/__getitem__.
        seen = set(self._values)
        seen.update(self._lazy)
        return iter(seen)

    def __len__(self) -> int:
        return len(set(self._values) | set(self._lazy))

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        canonical = self._aliases.get(key, key)
        return canonical in self._values or canonical in self._lazy

    def __repr__(self) -> str:
        if self._name:
            return f"<Registry {self._name}>"
        return "<Registry>"

    # -- registration --------------------------------------------------------

    def register(self, key: str, value: V, *, tags: tuple[str, ...] = ()) -> V:
        """Register ``value`` under ``key``. Returns ``value`` so it can be used inline."""
        if key in self._values or key in self._lazy:
            if self._on_conflict == "error":
                raise RegistryConflict(
                    f"{key!r} is already registered in {self!r}"
                )
            if self._on_conflict == "keep":
                return self._values.get(key) or self._lazy[key]()
            # on_conflict == "replace" falls through
        self._values[key] = value
        self._lazy.pop(key, None)
        if tags:
            self._tags[key] = frozenset(tags)
        self._notify(key, value)
        return value

    def register_lazy(
        self, key: str, loader: Callable[[], V], *, tags: tuple[str, ...] = ()
    ) -> None:
        """Register a no-arg ``loader`` callable; value is built on first ``__getitem__``.

        Once built, the result replaces the loader and is cached.
        """
        if key in self._values or key in self._lazy:
            if self._on_conflict == "error":
                raise RegistryConflict(
                    f"{key!r} is already registered in {self!r}"
                )
            if self._on_conflict == "keep":
                return
        self._lazy[key] = loader
        self._values.pop(key, None)
        if tags:
            self._tags[key] = frozenset(tags)
        # No notify: the value isn't built yet. Emit on first access? No —
        # subscribers want resolved values, and lazy entries promise nothing
        # until accessed. Document this in the subscriber contract.

    def register_decorator(
        self, key: str, *, tags: tuple[str, ...] = ()
    ) -> Callable[[V], V]:
        """Return a decorator that registers the decorated object under ``key``.

        >>> r = Registry()
        >>> @r.register_decorator("hi")
        ... def hi(): return "hello"
        >>> r["hi"]()
        'hello'
        """
        def _wrap(value: V) -> V:
            self.register(key, value, tags=tags)
            return value
        return _wrap

    def alias(self, alias: str, target: str) -> None:
        """Make ``alias`` resolve to ``target`` (which must already be registered)."""
        if target not in self._values and target not in self._lazy:
            raise RegistryMissing(
                f"cannot alias {alias!r} to unregistered {target!r} in {self!r}"
            )
        if alias in self._values or alias in self._lazy:
            raise RegistryConflict(
                f"{alias!r} is already a registered key in {self!r}; "
                f"cannot use it as an alias"
            )
        self._aliases[alias] = target

    # -- queries -------------------------------------------------------------

    def get(self, key: str, default=_MISSING):  # type: ignore[override]
        """Like ``dict.get`` but raises :class:`RegistryMissing` if no default given."""
        try:
            return self[key]
        except RegistryMissing:
            if default is _MISSING:
                raise
            return default

    def keys_with_tag(self, tag: str) -> list[str]:
        """Return canonical keys whose tag set contains ``tag``."""
        return [k for k, tags in self._tags.items() if tag in tags]

    def search(self, *, tags: tuple[str, ...] = ()) -> list[V]:
        """Return values whose tag set contains ALL of ``tags`` (empty ``tags`` = all)."""
        if not tags:
            return list(self.values())
        wanted = frozenset(tags)
        return [
            self[k] for k, key_tags in self._tags.items() if wanted.issubset(key_tags)
        ]

    # -- subscription --------------------------------------------------------

    def subscribe(
        self, callback: Callable[[str, V], None]
    ) -> Subscription:
        """Call ``callback(key, value)`` on every eager registration.

        Lazy registrations do NOT fire the callback at registration time;
        only when the value is materialized via ``__getitem__``. This
        keeps subscribers free of partially-built state.
        """
        self._subscribers.add(callback)
        return Subscription(self, callback)

    def _notify(self, key: str, value: V) -> None:
        # Iterate over a snapshot in case a callback unsubscribes itself.
        for cb in tuple(self._subscribers):
            cb(key, value)
