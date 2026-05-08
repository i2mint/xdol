"""Tests for xdol.registry."""

import pytest

from xdol.registry import (
    Registry,
    RegistryConflict,
    RegistryMissing,
)


def test_basic_register_and_get():
    r = Registry[int]()
    r.register("a", 1)
    assert r["a"] == 1
    assert "a" in r
    assert len(r) == 1


def test_get_missing_raises_with_default_returns():
    r = Registry()
    with pytest.raises(RegistryMissing):
        r.get("nope")
    assert r.get("nope", 42) == 42


def test_conflict_error():
    r = Registry()
    r.register("k", 1)
    with pytest.raises(RegistryConflict):
        r.register("k", 2)


def test_conflict_replace():
    r = Registry(on_conflict="replace")
    r.register("k", 1)
    r.register("k", 2)
    assert r["k"] == 2


def test_conflict_keep():
    r = Registry(on_conflict="keep")
    r.register("k", 1)
    r.register("k", 2)
    assert r["k"] == 1


def test_setitem_always_replaces():
    r = Registry(on_conflict="error")
    r["k"] = 1
    r["k"] = 2  # __setitem__ bypasses on_conflict by design
    assert r["k"] == 2


def test_alias_resolves():
    r = Registry()
    r.register("canonical", "value")
    r.alias("short", "canonical")
    assert r["short"] == "value"
    assert "short" in r


def test_alias_to_unregistered_fails():
    r = Registry()
    with pytest.raises(RegistryMissing):
        r.alias("a", "missing")


def test_alias_collides_with_existing_key():
    r = Registry()
    r.register("a", 1)
    r.register("b", 2)
    with pytest.raises(RegistryConflict):
        r.alias("a", "b")


def test_lazy_loading_called_once():
    calls = []
    r = Registry()
    r.register_lazy("k", lambda: calls.append(1) or "value")
    assert calls == []
    assert r["k"] == "value"
    assert r["k"] == "value"
    assert calls == [1]


def test_lazy_does_not_notify_until_accessed():
    seen = []
    r = Registry()
    r.subscribe(lambda k, v: seen.append((k, v)))
    r.register_lazy("k", lambda: "v")
    assert seen == []  # lazy not yet built
    _ = r["k"]
    # By design, lazy materialization does NOT fire subscribers
    # (subscribers wanted to know about *registrations*, and registration
    # happened earlier; the value was just deferred).
    assert seen == []


def test_subscribe_unsubscribe():
    r = Registry()
    seen = []
    sub = r.subscribe(lambda k, v: seen.append(k))
    r.register("a", 1)
    sub.unsubscribe()
    r.register("b", 2)
    assert seen == ["a"]


def test_decorator_registers_and_returns_func():
    r = Registry()

    @r.register_decorator("greet")
    def greet(name):
        return f"hi {name}"

    assert r["greet"]("ada") == "hi ada"
    # The decorator must return the original callable so further decoration works.
    assert greet("ada") == "hi ada"


def test_tags_search():
    r = Registry()
    r.register("a", 1, tags=("fast", "image"))
    r.register("b", 2, tags=("fast", "video"))
    r.register("c", 3, tags=("slow", "image"))
    assert sorted(r.search(tags=("fast",))) == [1, 2]
    assert sorted(r.search(tags=("image",))) == [1, 3]
    assert r.search(tags=("fast", "image")) == [1]
    # No tags = all values
    assert sorted(r.search()) == [1, 2, 3]


def test_keys_with_tag():
    r = Registry()
    r.register("a", 1, tags=("x",))
    r.register("b", 2, tags=("y",))
    assert r.keys_with_tag("x") == ["a"]


def test_delitem_removes_value_and_aliases():
    r = Registry()
    r.register("k", 1)
    r.alias("kk", "k")
    del r["k"]
    assert "k" not in r
    assert "kk" not in r


def test_delitem_via_alias_removes_canonical():
    r = Registry()
    r.register("k", 1)
    r.alias("kk", "k")
    del r["kk"]
    assert "k" not in r
    assert "kk" not in r


def test_delitem_missing_raises():
    r = Registry()
    with pytest.raises(RegistryMissing):
        del r["nope"]


def test_iter_yields_canonical_only():
    r = Registry()
    r.register("a", 1)
    r.register("b", 2)
    r.alias("aa", "a")
    assert sorted(r) == ["a", "b"]


def test_typed_generic_parameter_does_not_break_runtime():
    # Generic[V] should be a type-checking-only annotation.
    r: Registry[int] = Registry[int]()
    r.register("a", 1)
    assert r["a"] == 1


def test_repr_includes_name():
    r = Registry(name="tools")
    assert repr(r) == "<Registry tools>"
    r2 = Registry()
    assert repr(r2) == "<Registry>"


def test_callback_unsubscribing_during_notify_is_safe():
    r = Registry()
    seen = []

    def cb(k, v):
        seen.append(k)
        sub.unsubscribe()  # unsubscribe ourselves mid-iteration

    sub = r.subscribe(cb)
    r.register("a", 1)
    r.register("b", 2)  # cb already detached — should not be called again
    assert seen == ["a"]
