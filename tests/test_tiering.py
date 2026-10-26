"""Tests for xdol.tiering."""

import os
import warnings

import pytest
from dol import Files

from xdol import AccessLog, GdsfPolicy, HotWriteError, TieredStore
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
        clock=clock,
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
        {}, {}, budget_bytes=10, policy=GdsfPolicy(min_residency_s=0), tracker=log, clock=clock
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
    s["b"] = b"y" * 6
    with pytest.warns(UserWarning):
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
    root = tmp_path / "hot"
    root.mkdir()
    s, _ = make(0, hot=Files(str(root) + os.sep), cold={"a": b"data"})
    p = local_paths(s)["a"]
    assert open(p, "rb").read() == b"data"
    s2, _ = make(0)
    with pytest.raises(TypeError):
        local_paths(s2)


def test_default_tracker_warns_without_rootdir():
    with pytest.warns(UserWarning):
        TieredStore({}, {}, budget_bytes=None)
