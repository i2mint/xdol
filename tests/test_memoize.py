"""Tests for xdol.memoize: record/replay persistent memoization."""

import asyncio
import inspect
import json
import warnings

import pytest

from xdol.memoize import (
    AtomicFiles, CacheCorrupt, CacheMiss, CacheWriteError, file_cache_store,
    memoize_calls, recording_enabled,
)


@pytest.fixture(autouse=True)
def _no_ambient_record(monkeypatch):
    monkeypatch.delenv("XDOL_MEMOIZE_RECORD", raising=False)


class Counter:
    def __init__(self):
        self.n = 0

    def __call__(self, x, *, tag="t"):
        """Doc."""
        self.n += 1
        return {"x": x, "tag": tag}


def test_replay_is_default_and_a_miss_calls_nothing(tmp_path):
    f = Counter()
    m = memoize_calls(f, store=tmp_path)
    with pytest.raises(CacheMiss, match="replay-only"):
        m(1)
    assert f.n == 0


def test_record_then_replay_across_instances(tmp_path):
    f = Counter()
    rec = memoize_calls(f, store=tmp_path, mode="record")
    assert rec(1) == rec(1) == {"x": 1, "tag": "t"}
    assert f.n == 1
    g = Counter()
    replay = memoize_calls(g, store=tmp_path)  # fresh process, default mode
    assert replay(1) == {"x": 1, "tag": "t"} and g.n == 0


def test_env_opt_in_is_read_at_call_time(tmp_path, monkeypatch):
    f = Counter()
    m = memoize_calls(f, store=tmp_path)  # decorated before the env is set
    monkeypatch.setenv("XDOL_MEMOIZE_RECORD", "1")
    assert recording_enabled() and m.cache_mode() == "record"
    m(1)
    monkeypatch.delenv("XDOL_MEMOIZE_RECORD")
    assert m(1) == {"x": 1, "tag": "t"} and f.n == 1


def test_modes_refresh_and_off(tmp_path):
    f = Counter()
    memoize_calls(f, store=tmp_path, mode="record")(1)
    memoize_calls(f, store=tmp_path, mode="refresh")(1)
    assert f.n == 2
    memoize_calls(f, store=tmp_path, mode="off")(1)
    assert f.n == 3
    off = memoize_calls(f, store=tmp_path / "other", mode="off")
    off(1)
    assert not (tmp_path / "other").exists() or not list((tmp_path / "other").iterdir())
    with pytest.raises(ValueError, match="Unknown mode"):
        memoize_calls(f, store=tmp_path, mode="nope")(1)


def test_callable_mode(tmp_path):
    state = {"mode": "replay"}
    f = Counter()
    m = memoize_calls(f, store=tmp_path, mode=lambda: state["mode"])
    with pytest.raises(CacheMiss):
        m(1)
    state["mode"] = "record"
    m(1)
    state["mode"] = "replay"
    assert m(1)["x"] == 1


def test_wraps_preserves_signature_and_metadata(tmp_path):
    def add(a: int, b: int = 2) -> int:
        """Adds."""
        return a + b

    m = memoize_calls(add, store=tmp_path, mode="record")
    assert m.__name__ == "add" and m.__doc__ == "Adds."
    assert inspect.signature(m) == inspect.signature(add)
    assert m.__wrapped__ is add


def test_decorator_form_and_drop(tmp_path):
    seen = []

    @memoize_calls(store=tmp_path, mode="record", drop=["logger"])
    def f(prompt, logger=None):
        seen.append(prompt)
        return prompt.upper()

    assert f("a", logger=print) == f("a", logger=None) == f(prompt="a") == "A"
    assert seen == ["a"]


def test_key_func_and_drop_are_exclusive(tmp_path):
    with pytest.raises(ValueError, match="either"):
        memoize_calls(lambda a: a, store=tmp_path, key_func=str, drop=["a"])


def test_exceptions_are_not_cached(tmp_path):
    calls = []

    def flaky(x):
        calls.append(x)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return x

    m = memoize_calls(flaky, store=tmp_path, mode="record")
    with pytest.raises(RuntimeError):
        m(1)
    assert m(1) == 1 and calls == [1, 1]


def test_none_and_falsy_results_are_cached(tmp_path):
    n = []

    def f(x):
        n.append(x)

    m = memoize_calls(f, store=tmp_path, mode="record")
    assert m(1) is None and m(1) is None and n == [1]


def test_binary_codec_and_paired_codec(tmp_path):
    img = memoize_calls(lambda s: s.encode(), store=tmp_path / "b", codec="bytes", mode="record")
    assert img("x") == b"x"
    assert img("x") == b"x"

    n = []

    def render(prompt):
        n.append(prompt)
        return {"prompt": prompt}, b"\x89PNG\x00"

    m = memoize_calls(render, store=tmp_path / "p", codec="paired", mode="record")
    assert m("cat") == m("cat") == ({"prompt": "cat"}, b"\x89PNG\x00")
    assert n == ["cat"]
    names = sorted(p.suffix for p in (tmp_path / "p").iterdir())
    assert names == [".bin", ".json"]


def test_bytes_store_gets_codec_applied_on_top():
    raw = {}  # stands in for S3
    m = memoize_calls(lambda x: [x], store=raw, codec="json", mode="record")
    m(1)
    (key,) = raw
    assert json.loads(raw[key]) == [1]
    # an object-valued mapping needs no codec
    objs = {}
    memoize_calls(lambda x: [x], store=objs, mode="record")(1)
    assert list(objs.values()) == [[1]]


def test_unserializable_result_raises_with_result_attached_or_warns(tmp_path):
    m = memoize_calls(lambda: {1, 2}, store=tmp_path, mode="record")
    with pytest.raises(CacheWriteError) as ei:
        m()
    assert ei.value.result == {1, 2}
    w = memoize_calls(lambda: {1, 2}, store=tmp_path, mode="record", on_write_error="warn")
    with pytest.warns(UserWarning, match="could not be stored"):
        assert w() == {1, 2}
    assert list(tmp_path.iterdir()) == []  # nothing half-written


def test_corrupt_entry(tmp_path):
    f = Counter()
    m = memoize_calls(f, store=tmp_path, mode="record")
    m(1)
    (path,) = tmp_path.iterdir()
    path.write_bytes(b"{truncated")
    with pytest.raises(CacheCorrupt):
        memoize_calls(f, store=tmp_path)(1)
    with pytest.warns(UserWarning, match="undecodable"):
        assert m(1)["x"] == 1  # recording heals it
    assert memoize_calls(f, store=tmp_path)(1)["x"] == 1


def test_store_failures_are_not_mistaken_for_misses():
    class Flaky(dict):
        def __getitem__(self, k):
            raise ConnectionError("network down")

    f = Counter()
    m = memoize_calls(f, store=Flaky(), mode="record")
    with pytest.raises(ConnectionError):
        m(1)
    assert f.n == 0  # no real call (no spend) because the store was unreachable


def test_async_functions(tmp_path):
    n = []

    async def f(x):
        n.append(x)
        return x * 2

    m = memoize_calls(f, store=tmp_path, mode="record")
    assert inspect.iscoroutinefunction(m)
    assert asyncio.run(m(2)) == asyncio.run(m(2)) == 4 and n == [2]
    with pytest.raises(CacheMiss):
        asyncio.run(memoize_calls(f, store=tmp_path / "x")(2))


def test_atomic_files_hides_temp_and_cleans_up_on_failure(tmp_path):
    store = AtomicFiles(str(tmp_path))
    store["a"] = b"1"
    store["sub/b"] = b"2"
    (tmp_path / "c.123.xdol-tmp").write_bytes(b"stale")
    assert sorted(k.replace("\\", "/") for k in store) == ["a", "sub/b"]  # os.sep
    with pytest.raises(TypeError):
        store["d"] = 123  # not bytes
    assert not [p for p in tmp_path.iterdir() if p.name.startswith("d")]


def test_file_cache_store_is_reviewable_json(tmp_path):
    store = file_cache_store(tmp_path / "cassettes")
    store["k"] = {"b": 1, "a": "é"}
    text = (tmp_path / "cassettes" / "k").read_text(encoding="utf-8")
    assert text.index('"a"') < text.index('"b"') and "é" in text


def test_method_needs_drop_self(tmp_path):
    class Client:
        @staticmethod
        def make():
            return Client()

        def __init__(self):
            self.n = 0

        def call(self, x):
            self.n += 1
            return x

    c = Client()
    from xdol.keys import UnstableKeyError

    bad = memoize_calls(Client.call, store=tmp_path, mode="record")
    with pytest.raises(UnstableKeyError):
        bad(c, 1)
    good = memoize_calls(Client.call, store=tmp_path, mode="record", drop=["self"])
    assert good(c, 1) == good(Client(), 1) == 1 and c.n == 1


# --- regressions from the adversarial review of PR #10 ---


def test_failures_shaped_like_keyerror_or_valueerror_never_cost_a_call():
    class Archived(KeyError):
        pass

    class Misconfigured(ValueError):
        pass

    class Store(dict):
        def __getitem__(self, k):
            raise self.error(k)

        def __contains__(self, k):  # the entry *is* there, just unreadable now
            return True

    for err in (Archived, Misconfigured):
        s = Store()
        s.error = err
        f = Counter()
        with pytest.raises(err):
            memoize_calls(f, store=s, mode="record")(1)
        with pytest.raises(err):
            memoize_calls(f, store=s)(1)  # replay: not CacheCorrupt either
        assert f.n == 0


def test_keyerror_subclass_is_a_miss_when_the_store_agrees():
    class NotFound(KeyError):
        pass

    class Store(dict):
        def __getitem__(self, k):
            if k not in self.keys():
                raise NotFound(k)
            return super().__getitem__(k)

    f = Counter()
    m = memoize_calls(f, store=Store(), mode="record")
    assert m(1) == m(1) and f.n == 1


def test_record_returns_what_replay_returns(tmp_path):
    f = lambda: (1, {1: "a"})  # noqa: E731
    rec = memoize_calls(f, store=tmp_path, mode="record")()
    rep = memoize_calls(f, store=tmp_path)()
    assert rec == rep == [1, {"1": "a"}]
    raw = memoize_calls(f, store=tmp_path / "x", mode="record", reread_after_write=False)()
    assert raw == (1, {1: "a"})


def test_object_mapping_store_does_not_alias_results():
    cache = {}
    m = memoize_calls(lambda x: {"v": [x]}, store=cache, mode="record")
    r = m(1)
    r["v"].append("MUTATED")
    m(1)["v"].append("MUTATED2")
    assert m(1) == {"v": [1]}


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_sync_function_returning_a_coroutine_is_rejected(tmp_path):
    async def inner():
        return 1

    def outer():
        return inner()

    m = memoize_calls(outer, store=tmp_path, mode="record")
    with pytest.raises(TypeError, match="cannot be cached"):
        m()


def test_atomic_files_cannot_escape_root(tmp_path):
    root = tmp_path / "root"
    store = AtomicFiles(str(root))
    root.mkdir(exist_ok=True)
    with pytest.raises(ValueError, match="outside"):
        store["../escaped"] = b"x"
    assert "../escaped" not in store
    assert not (tmp_path / "escaped").exists()
    store["ok/inside"] = b"y"
    assert store["ok/inside"] == b"y"


def test_corrupt_paired_entry_is_corrupt_not_a_chimera(tmp_path):
    f = lambda: ({"seed": "A"}, b"asset-A")  # noqa: E731
    memoize_calls(f, store=tmp_path, codec="paired", mode="record")()
    (bin_path,) = [p for p in tmp_path.iterdir() if p.suffix == ".bin"]
    bin_path.write_bytes(b"asset-B")  # another writer's asset under A's metadata
    with pytest.raises(CacheCorrupt, match="do not belong together"):
        memoize_calls(f, store=tmp_path, codec="paired")()


def test_unreadable_but_present_entry_never_costs_a_call(tmp_path):
    f = Counter()
    m = memoize_calls(f, store=tmp_path, mode="record")
    m(1)
    (path,) = tmp_path.iterdir()
    path.unlink()
    path.mkdir()  # present but not a readable file -> treated as absent, see below
    # A genuine I/O failure (not a missing file) must propagate, not read as a miss:
    from unittest import mock

    with mock.patch("pathlib.Path.read_bytes", side_effect=OSError("EMFILE")):
        with pytest.raises(OSError, match="EMFILE"):
            m(1)
    assert f.n == 1


def test_traversal_key_fails_before_calling(tmp_path):
    f = Counter()
    m = memoize_calls(f, store=tmp_path, mode="record", key_func=lambda *a, **k: "../x")
    with pytest.raises(ValueError, match="outside"):
        m(1)
    assert f.n == 0


def test_uncopyable_result_is_still_cached_in_a_dict_store():
    import threading

    f = Counter()
    n = []

    def g():
        n.append(1)
        return {"lock": threading.Lock()}

    m = memoize_calls(g, store={}, mode="record")
    m(), m()
    assert len(n) == 1


def test_unreadable_paired_entry_never_costs_a_call(tmp_path):
    from unittest import mock

    f = lambda: ({"a": 1}, b"x")  # noqa: E731
    m = memoize_calls(f, store=tmp_path, codec="paired", mode="record")
    m()
    calls = []
    g = lambda: calls.append(1) or ({"a": 1}, b"x")  # noqa: E731
    m2 = memoize_calls(g, store=tmp_path, codec="paired", mode="record")
    with mock.patch("pathlib.Path.read_bytes", side_effect=PermissionError("EACCES")):
        with pytest.raises(PermissionError):
            m2()
    assert calls == []


def test_relative_store_path_is_pinned_at_decoration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    f = Counter()
    m = memoize_calls(f, store="cass", mode="record")
    m(3)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    m(3)
    assert f.n == 1 and not (elsewhere / "cass").exists()


def test_atomic_files_directory_style_key_is_absent_for_get_and_in(tmp_path):
    # i2mint/xdol#13: 'a/' used to read file 'a' while `'a/' in store` was False.
    from xdol.memoize import AtomicFiles

    store = AtomicFiles(str(tmp_path))
    store["a"] = b"x"
    assert "a/" not in store
    with pytest.raises(KeyError):
        store["a/"]
