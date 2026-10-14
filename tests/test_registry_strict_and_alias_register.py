"""Registry: opt-in ``strict_setitem`` and alias-aware ``register`` (xdol#6)."""

import pytest

from xdol.registry import Registry, RegistryConflict


def _aliased(**kw):
    reg = Registry(**kw)
    reg.register("canonical", 1)
    reg.alias("short", "canonical")
    return reg


# -- part 1: strict_setitem ---------------------------------------------------


def test_default_setitem_still_replaces_silently():
    reg = Registry()  # on_conflict='error', but setitem is explicit assignment
    reg["k"] = 1
    reg["k"] = 2
    assert reg["k"] == 2


def test_strict_setitem_error_policy_raises_on_existing_key():
    reg = Registry(strict_setitem=True)
    reg["k"] = 1
    with pytest.raises(RegistryConflict):
        reg["k"] = 2
    assert reg["k"] == 1


def test_strict_setitem_keep_and_replace_policies():
    keep = Registry(on_conflict="keep", strict_setitem=True)
    keep["k"] = 1
    keep["k"] = 2
    assert keep["k"] == 1
    replace = Registry(on_conflict="replace", strict_setitem=True)
    replace["k"] = 1
    replace["k"] = 2
    assert replace["k"] == 2


def test_strict_setitem_through_alias_hits_canonical_policy():
    reg = _aliased(strict_setitem=True)
    with pytest.raises(RegistryConflict, match="canonical"):
        reg["short"] = 2
    assert reg["canonical"] == 1


def test_strict_setitem_conflicts_with_lazy_entry_and_new_key_notifies():
    reg = Registry(strict_setitem=True)
    reg.register_lazy("lz", lambda: 1)
    with pytest.raises(RegistryConflict):
        reg["lz"] = 2
    seen = []
    reg.subscribe(lambda k, v: seen.append((k, v)))
    reg["new"] = 3
    assert seen == [("new", 3)]


def test_strict_setitem_makes_update_strict():
    reg = Registry(strict_setitem=True)
    reg["a"] = 1
    with pytest.raises(RegistryConflict):
        reg.update({"b": 2, "a": 3})


# -- part 2: register / register_lazy resolve aliases ---------------------------


def test_register_alias_key_raises_under_error_policy():
    reg = _aliased()
    with pytest.raises(RegistryConflict, match="alias"):
        reg.register("short", 2)
    assert reg["canonical"] == 1 and len(reg) == 1


def test_register_lazy_alias_key_raises_under_error_policy():
    reg = _aliased()
    with pytest.raises(RegistryConflict):
        reg.register_lazy("short", lambda: 2)
    assert reg["short"] == 1


def test_register_alias_key_replace_policy_updates_canonical_no_shadow():
    reg = _aliased(on_conflict="replace")
    assert reg.register("short", 2, tags=("t",)) == 2
    assert reg["canonical"] == 2 and reg["short"] == 2
    assert sorted(reg) == ["canonical"]
    assert reg.keys_with_tag("t") == ["canonical"]


def test_register_alias_key_keep_policy_returns_canonical_value():
    reg = _aliased(on_conflict="keep")
    assert reg.register("short", 2) == 1
    reg.register_lazy("short", lambda: 3)
    assert reg["canonical"] == 1


def test_keep_policy_returns_falsy_and_lazy_existing_values():
    reg = Registry(on_conflict="keep")
    reg.register("zero", 0)
    assert reg.register("zero", 5) == 0
    reg.register_lazy("lz", lambda: "built")
    assert reg.register("lz", "other") == "built"


def test_strict_keep_does_not_run_lazy_loader():
    calls = []
    reg = Registry(on_conflict="keep", strict_setitem=True)
    reg.register_lazy("lz", lambda: calls.append(1) or "built")
    reg["lz"] = "other"
    assert calls == [] and reg["lz"] == "built"


def test_setitem_works_without_init_state():
    reg = Registry.__new__(Registry)
    reg.__dict__.update(Registry().__dict__)
    del reg.__dict__["_strict_setitem"]
    reg["k"] = 1
    assert reg["k"] == 1
