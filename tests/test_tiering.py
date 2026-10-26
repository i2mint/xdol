"""Tests for xdol.tiering."""

import os
import warnings

import pytest
from dol import Files
from dol.trash import permanent_delete

from xdol import (
    AccessLog,
    AccessStats,
    GdsfPolicy,
    HotWriteError,
    LruPolicy,
    TieredStore,
)
from xdol.tiering import local_paths, pin, prefetch, status, sweep, unpin

DAY = 86400


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def make(budget, *, hot=None, cold=None, clock=None, **policy):
    clock = clock or Clock()
    log = AccessLog(clock=clock, tag="t")
    store = TieredStore(
        {} if hot is None else hot,
        {} if cold is None else cold,
        budget_bytes=budget,
        policy=GdsfPolicy(**policy),
        tracker=log,
    )
    return store, clock


def test_write_through_and_read():
    s, _ = make(None)
    s["a"] = b"xx"
    assert s.cold["a"] == s.hot["a"] == b"xx"
    assert s["a"] == b"xx" and len(s) == 1 and list(s) == ["a"]
    del s["a"]
    assert "a" not in s.hot and "a" not in s.cold


def test_cold_raises_hot_untouched_nothing_recorded():
    class Bad(dict):
        def __setitem__(self, k, v):
            raise OSError("down")

    s, _ = make(None, cold=Bad())
    with pytest.raises(OSError):
        s["a"] = b"x"
    assert s.hot == {} and len(s.tracker) == 0


def test_hot_write_error():
    class Bad(dict):
        def __setitem__(self, k, v):
            raise OSError("full")

    s, _ = make(None, hot=Bad())
    with pytest.raises(HotWriteError):
        s["a"] = b"x"
    assert s.cold["a"] == b"x"
    assert s.hot == {} and status(s)["hot_bytes"] == 0
    assert s.tracker["a"].writes == 1


def test_budget_zero_is_cold_only():
    s, _ = make(0)
    s["a"] = b"xx"
    assert s.hot == {} and s["a"] == b"xx" and s["a"] == b"xx"
    assert s.hot == {}


def test_budget_none_admits_on_first_read():
    s, _ = make(None, cold={"a": b"x"})
    assert s["a"] == b"x" and s.hot == {"a": b"x"}


def test_second_hit_admission():
    s, clock = make(1000, cold={"a": b"x"}, admit_window_s=10 * DAY)
    s["a"]
    assert s.hot == {}  # first read: no admission
    clock.t += DAY
    s["a"]
    assert s.hot == {"a": b"x"}  # second read inside the window
    s2, c2 = make(1000, cold={"a": b"x"}, admit_window_s=10 * DAY)
    s2["a"]
    c2.t += 11 * DAY
    s2["a"]
    assert s2.hot == {}  # outside the window


def test_sweep_min_residency_and_priority():
    s, clock = make(10, min_residency_s=DAY)
    s["big"] = b"x" * 6
    with pytest.warns(UserWarning):
        s["small"] = b"y" * 3  # 9 > 8.5 but all within min residency
    assert set(s.hot) == {"big", "small"}
    clock.t += 2 * DAY
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        report = sweep(s)
    # big has lower frequency/size -> released first; 3 <= 7.5
    assert report.keys_released == ["big"]
    assert (report.bytes_before, report.bytes_after) == (9, 3)
    assert set(s.cold) == {"big", "small"}
    assert s.tracker.stats("big").size == 6


def test_inline_release_on_write():
    s, clock = make(10, min_residency_s=0)
    clock.t += 1
    s["a"] = b"x" * 6
    s["b"] = b"y" * 6  # 12 > 8.5 -> sweep to <= 7.5
    assert len(s.hot) == 1 and len(s.cold) == 2


def test_pins_protect_and_survive_reopen(tmp_path):
    clock = Clock()
    path = tmp_path / "log.jsonl"
    log = AccessLog(path, clock=clock)
    s = TieredStore(
        {}, {}, budget_bytes=10, policy=GdsfPolicy(min_residency_s=0), tracker=log
    )
    s["a"] = b"x" * 6
    pin(s, "a")
    log.close()
    assert AccessLog(path).stats("a").pinned
    s["b"] = b"y" * 6  # inline sweep releases b, never the pinned a
    assert set(s.hot) == {"a"} and set(s.cold) == {"a", "b"}
    unpin(s, "a")
    assert not s.tracker.stats("a").pinned


def test_all_protected_warns():
    s, _ = make(10)  # default 7-day residency protects everything
    s["a"] = b"x" * 6
    with pytest.warns(UserWarning, match="protected"):
        s["b"] = b"y" * 6  # the inline sweep warns too
    with pytest.warns(UserWarning, match="protected"):
        sweep(s)
    assert len(s.hot) == 2


def test_contains_iter_len_record_nothing():
    s, _ = make(None, cold={"a": b"x"})
    "a" in s, list(s), len(s)
    assert len(s.tracker) == 0


def test_status_and_prefetch():
    s, _ = make(100, cold={"a": b"xx", "b": b"y"})
    prefetch(s, ["a"])
    st = status(s)
    assert st["hot_bytes"] == 2 and st["cold_keys"] == 2 and st["hot_keys"] == 1


def test_local_paths_with_files_hot(tmp_path):
    s, _ = make(0, hot=_files(tmp_path), cold={"a": b"data"})
    p = local_paths(s)["a"]
    assert open(p, "rb").read() == b"data"
    s2, _ = make(0)
    with pytest.raises(TypeError):
        local_paths(s2)


def test_default_tracker_warns_without_rootdir():
    with pytest.warns(UserWarning):
        TieredStore({}, {}, budget_bytes=None)


# -- review fixes ---------------------------------------------------------------


def _files(tmp_path, name="hot", **contents):
    root = tmp_path / name
    root.mkdir()
    for k, v in contents.items():
        (root / k).write_bytes(v)
    # permanent deletion: the default trash needs pywin32 on Windows and warns
    return Files(str(root) + os.sep, delete_func=permanent_delete)


def test_gdsf_aging_raises_l_and_new_keys_inherit_it():
    p = GdsfPolicy(min_residency_s=0)
    st = AccessStats(size=1, reads=1, last_access=1.0)
    p.on_hot_access("a", st, admitted=True)
    p.on_hot_access("b", st, admitted=True)
    p.on_hot_access("b", st, admitted=False)  # F=2 -> H=2
    hot = {"a": st, "b": st}
    released = list(p.to_release(hot, hot_bytes=10, budget_bytes=10, now=2.0))
    assert released == ["a", "b"] and p._aging == 2.0
    often_read = AccessStats(size=1, reads=50, last_access=1.0)
    p.on_hot_access("c", often_read, admitted=True)  # F counts since admission
    assert p._entries["c"] == (1, 3.0)  # H = L + F * cost / size


def test_old_frequent_key_is_eventually_released():
    s, clock = make(10, cold={"old": b"o"}, min_residency_s=0, admit_after_reads=1)
    for _ in range(100):
        s["old"]
    for i in range(400):
        clock.t += 1
        s.cold[f"n{i}"] = b"n"
        s[f"n{i}"], s[f"n{i}"]
    assert "old" not in s.hot and "old" in s.cold


def test_admit_after_reads_one_admits_on_first_read():
    s, _ = make(1000, cold={"a": b"x"}, admit_after_reads=1)
    s["a"]
    assert s.hot == {"a": b"x"}


def test_item_larger_than_budget_lands_in_cold_only():
    s, _ = make(10, min_residency_s=0)
    s["big"] = b"x" * 20
    assert s.hot == {} and s["big"] == b"x" * 20 and status(s)["hot_bytes"] == 0


def test_overwrite_and_delete_bookkeeping():
    s, _ = make(100)
    s["a"] = b"x" * 6
    s["a"] = b"y" * 2
    assert status(s)["hot_bytes"] == 2 and s.tracker["a"].size == 2
    assert s.tracker["a"].writes == 2
    del s["a"]
    assert status(s)["hot_bytes"] == 0 and status(s)["hot_keys"] == 0
    assert "a" not in s.tracker


def test_budget_zero_overwrite_drops_stale_hot_copy():
    s, _ = make(0, hot={"a": b"old"}, cold={"a": b"old"})
    s["a"] = b"new"
    assert "a" not in s.hot and s["a"] == b"new"


def test_unsized_value_raises_before_any_write():
    s, _ = make(None)
    with pytest.raises(TypeError, match="len"):
        s["a"] = 42
    assert s.cold == {} and s.hot == {} and len(s.tracker) == 0


def test_failed_hot_removal_keeps_record_in_cold():
    class Stuck(dict):
        def __delitem__(self, k):
            raise OSError("busy")

    s, _ = make(None)
    s["a"] = b"x"
    s._hot = Stuck(s.hot)
    with pytest.raises(OSError):
        del s["a"]
    assert s.cold["a"] == b"x" and s["a"] == b"x"


def test_startup_with_existing_hot_store(tmp_path):
    s, _ = make(100, hot={"a": b"xxx"}, cold={"a": b"xxx"})
    assert len(s.tracker) == 0  # nothing recorded at startup
    assert status(s)["hot_bytes"] == 0  # unknown size, learnt lazily...
    s["a"]
    assert status(s)["hot_bytes"] == 3  # ...on the next hot hit
    s, _ = make(100, hot=_files(tmp_path, f=b"12345"), cold={"f": b"12345"})
    assert status(s)["hot_bytes"] == 5 and len(s.tracker) == 0  # the file size
    log = AccessLog(tag="t")
    log.record("a", "write", 7)
    s = TieredStore({"a": b"x"}, {}, budget_bytes=100, tracker=log)
    assert status(s)["hot_bytes"] == 7  # the size in the log


def test_unknown_startup_keys_are_releasable(tmp_path):
    s, _ = make(10, hot=_files(tmp_path, a=b"x" * 6), cold={"a": b"x" * 6})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        s["b"] = b"y" * 6  # 12 > 8.5: a has no recorded access, b is resident
    assert set(s.hot) == {"b"} and "a" not in s.tracker  # release adds no entry


def test_local_paths_bounded_store(tmp_path):
    cold = {"a": b"x" * 6, "b": b"y" * 6}
    s, _ = make(10, hot=_files(tmp_path), cold=cold, min_residency_s=0)
    paths = local_paths(s)
    assert open(paths["a"], "rb").read() == cold["a"]
    for _ in range(3):
        s["a"]  # a now outranks b
    p = paths["b"]  # 12 > 8.5: the release must spare b anyway
    assert open(p, "rb").read() == cold["b"] and set(s.hot) == {"b"}
    assert s.tracker["b"].reads == 1 and s.tracker["b"].last_read > 0


def test_local_paths_raises_when_not_materialised(tmp_path):
    class Sink(Files):
        def __setitem__(self, k, v):
            pass

    root = tmp_path / "hot"
    root.mkdir()
    s, _ = make(None, hot=Sink(str(root) + os.sep), cold={"a": b"x"})
    with pytest.raises(FileNotFoundError):
        local_paths(s)["a"]


def test_policy_entries_do_not_leak_on_delete():
    s, _ = make(None, cold={})
    for i in range(20):
        s[f"k{i}"] = b"x"
        del s[f"k{i}"]
    assert s.policy._entries == {}


def test_release_does_not_read_the_hot_value(tmp_path):
    class NoReadFiles(Files):
        def __getitem__(self, k):
            raise AssertionError("release must not read the value")

    root = tmp_path / "hot"
    root.mkdir()
    hot = NoReadFiles(str(root) + os.sep, delete_func=permanent_delete)
    s, _ = make(None, hot=hot, cold={})
    s["a"] = b"x"
    s._drop_hot("a")
    assert list(hot) == []


def make_lru(budget, **kw):
    clock = Clock()
    kw = {"min_residency_s": 0, "admit_after_reads": 1, **kw}
    s = TieredStore(
        {},
        {},
        budget_bytes=budget,
        policy=LruPolicy(**kw),
        tracker=AccessLog(clock=clock),
    )
    return s, clock


def test_lru_hot_hit_refreshes_recency():
    s, clock = make_lru(20)
    s["a"] = b"x" * 6
    clock.t += 1
    s["b"] = b"x" * 6
    clock.t += 1
    s["a"]  # hot hit: a is now more recent than b
    clock.t += 1
    s["c"] = b"x" * 6  # 18 > 17
    assert set(s.hot) == {"a", "c"}


def test_lru_ignores_frequency():
    s, clock = make_lru(20)
    s["a"] = b"x" * 6
    s["b"] = b"x" * 6
    for _ in range(5):
        clock.t += 1
        s["b"]  # b is read often, but long ago
    clock.t += 10
    s["a"]  # a is read once, most recently (GDSF would keep b)
    clock.t += 1
    s["c"] = b"x" * 6
    assert set(s.hot) == {"a", "c"}


def test_lru_respects_pins_and_residency():
    s, clock = make_lru(10)
    s["a"] = b"x" * 6
    pin(s, "a")
    clock.t += 1
    s["b"] = b"y" * 6  # over budget: the pinned a is spared
    assert set(s.hot) == {"a"}
    s2 = TieredStore(
        {}, {}, budget_bytes=10, policy=LruPolicy(), tracker=AccessLog(clock=Clock())
    )
    s2["a"] = b"x" * 6
    with pytest.warns(UserWarning, match="protected"):
        s2["b"] = b"y" * 6  # default residency protects both
    assert len(s2.hot) == 2


def test_local_paths_access_counts_as_use(tmp_path):
    s, clock = make(100, hot=_files(tmp_path), cold={"a": b"xx"})
    paths = local_paths(s)
    paths["a"]
    before = s.tracker["a"]
    clock.t += 5
    paths["a"]  # already hot: still recorded as a use
    assert s.tracker["a"].reads == before.reads + 1
    assert s.tracker["a"].last_access > before.last_access


def test_local_paths_default_rejects_relative_path(tmp_path):
    class Wrapped(dict):  # like a wrap_kvs-wrapped Files: ids are not full paths
        rootdir = str(tmp_path) + os.sep

        def _id_of_key(self, key):
            return key

    hot = Wrapped()
    s, _ = make(None, hot=hot, cold={"a": b"x"})
    with pytest.raises(TypeError, match="path_of"):
        local_paths(s)["a"]


def test_local_paths_path_of_and_contains(tmp_path):
    s, _ = make(None, hot=_files(tmp_path), cold={"a": b"data"})
    seen = []

    def path_of(key):
        seen.append(key)
        return os.path.join(s.hot.rootdir, key)

    paths = local_paths(s, path_of=path_of)
    assert "a" in paths and "zz" not in paths
    assert "a" not in s.hot  # `in` did not fetch
    assert open(paths["a"], "rb").read() == b"data" and seen == ["a"]
    # a hot store without rootdir works when path_of is given
    s2, _ = make(None, cold={"a": b"d"})
    p = tmp_path / "elsewhere"
    p.write_bytes(b"d")
    assert local_paths(s2, path_of=lambda k: str(p))["a"] == str(p)


def test_local_paths_relative_rootdir_ok(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    s, _ = make(None, hot=Files("rel" + os.sep), cold={"a": b"x"})
    os.mkdir("rel")
    assert open(local_paths(s)["a"], "rb").read() == b"x"


def test_local_paths_path_outside_root_rejected(tmp_path):
    # default guard: a hot store whose ids point outside its rootdir
    class Out(dict):
        rootdir = str(tmp_path / "root") + os.sep

        def _id_of_key(self, key):
            return str(tmp_path / "other" / key)

    s2, _ = make(None, hot=Out(), cold={"a": b"x"})
    with pytest.raises(TypeError, match="path_of"):
        local_paths(s2)["a"]
    assert "a" not in s2.hot  # rejected before fetching


def test_local_paths_refetches_file_deleted_behind_our_back(tmp_path):
    s, _ = make(None, hot=_files(tmp_path), cold={"a": b"x"})
    paths = local_paths(s)
    p = paths["a"]
    os.remove(p)
    assert open(paths["a"], "rb").read() == b"x"


def test_local_paths_use_bumps_gdsf_frequency(tmp_path):
    s, _ = make(None, hot=_files(tmp_path), cold={"a": b"x"})
    paths = local_paths(s)
    paths["a"]
    f0 = s._policy._entries["a"][0]
    paths["a"]
    assert s._policy._entries["a"][0] == f0 + 1


def test_gdsf_forgets_entries_of_keys_no_longer_hot():
    p = GdsfPolicy(min_residency_s=0)
    p.on_hot_access("gone", AccessStats(size=1), admitted=True)
    assert list(p.to_release({}, hot_bytes=100, budget_bytes=10, now=0)) == []
    assert "gone" not in p._entries


def test_budget_zero_sweep_releases_everything_even_protected():
    s, _ = make(0, cold={"a": b"xx", "b": b"yy"})  # default residency protects
    prefetch(s, ["a", "b"])
    pin(s, "a")
    assert set(s.hot) == {"a", "b"}
    report = sweep(s)
    assert s.hot == {} and set(report.keys_released) == {"a", "b"}
    assert set(s.cold) == {"a", "b"}


def test_budget_zero_sweep_spares_excluded():
    s, _ = make(0, cold={"a": b"xx", "b": b"yy"})
    prefetch(s, ["a", "b"])
    sweep(s, exclude=["a"])
    assert set(s.hot) == {"a"}


def test_budget_zero_sweep_accepts_generator_exclude():
    s, _ = make(0, cold={"a": b"xx", "b": b"yy", "c": b"zz"})
    prefetch(s, ["a", "b", "c"])
    sweep(s, exclude=(k for k in ["a", "b"]))
    assert set(s.hot) == {"a", "b"}


def test_gdsf_after_restart_long_history_does_not_outrank_recent_hits(tmp_path):
    clock = Clock()
    path = tmp_path / "log.jsonl"
    hot, cold = {}, {}

    def open_store():
        return TieredStore(
            hot,
            cold,
            budget_bytes=10,
            policy=GdsfPolicy(min_residency_s=0, admit_after_reads=1),
            tracker=AccessLog(path, clock=clock),
        )

    s = open_store()
    s["old"] = b"o" * 4
    for _ in range(20):
        s["old"]  # long history, all before the restart
    s.tracker.close()

    clock.t += DAY
    s = open_store()  # restart: "old" is hot, its policy entry is gone
    s["new"] = b"n" * 4
    s["new"], s["new"]  # recent hits
    s["c"] = b"c" * 2  # crosses the high watermark: one or more keys must go
    assert "old" not in s.hot and "new" in s.hot
