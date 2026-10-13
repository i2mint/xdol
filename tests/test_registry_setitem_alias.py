"""``Registry.__setitem__`` resolves aliases like ``__getitem__``/``__delitem__`` (xdol#5)."""

from xdol.registry import Registry


def _aliased():
    reg = Registry()
    reg.register("canonical", 1)
    reg.alias("short", "canonical")
    return reg


def test_setitem_through_alias_updates_canonical():
    reg = _aliased()
    reg["short"] = 2
    assert reg["canonical"] == 2
    assert reg["short"] == 2
    assert sorted(reg) == ["canonical"]
    assert len(reg) == 1


def test_setitem_through_alias_leaves_no_shadow_after_delete():
    reg = _aliased()
    reg["short"] = 2
    del reg["short"]
    assert len(reg) == 0
    assert "canonical" not in reg and "short" not in reg


def test_setitem_through_alias_replaces_lazy_entry_and_notifies():
    reg = Registry()
    reg.register_lazy("canonical", lambda: "lazy")
    reg.alias("short", "canonical")
    seen = []
    reg.subscribe(lambda k, v: seen.append((k, v)))
    reg["short"] = "eager"
    assert reg["canonical"] == "eager"
    assert seen == [("canonical", "eager")]


def test_setitem_plain_key_unchanged():
    reg = Registry()
    reg["k"] = 1
    reg["k"] = 2
    assert reg["k"] == 2
