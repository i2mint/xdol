"""Tests for xdol.access."""

import os
import warnings

import pytest
from dol import Files

from xdol import AccessLog, track_access, merge_stats, default_access_log_for


class Clock:
    def __init__(self, t=100.0):
        self.t = t

    def __call__(self):
        self.t += 1
        return self.t


def test_record_reopen_same_stats(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock(), tag="m1")
    log.record("a", "write", 5)
    log.record("a", "read", 5, served_from="hot")
    log.record("a", "pin")
    log.close()
    again = AccessLog(p)
    assert again.stats("a") == log.stats("a")
    assert again.stats("a").pinned and again.stats("a").frequency == 2
    assert list(again) == ["a"] and len(again) == 1


def test_compact_preserves_and_shrinks(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock())
    for _ in range(20):
        log.record("a", "read", 3)
    log.record("b", "write", 1)
    before = os.path.getsize(p)
    expected = {k: log.stats(k) for k in log}
    log.compact()
    assert os.path.getsize(p) < before
    log.record("a", "read", 3)  # still appendable after compact
    log.close()
    again = AccessLog(p)
    assert again.stats("b") == expected["b"]
    assert again.stats("a").reads == 21


def test_unwritable_warns_once_and_counts(tmp_path):
    log = AccessLog(tmp_path / "no_such_dir" / "log.jsonl")
    with pytest.warns(UserWarning):
        log.record("a", "read", 1)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        log.record("a", "read", 1)
    assert log.stats("a").reads == 2


def test_contains_records_nothing():
    log = AccessLog()
    s = track_access({"a": b"x"}, tracker=log)
    assert "a" in s and list(s) == ["a"] and len(s) == 1
    assert len(log) == 0


def test_merge_stats_across_tags():
    l1, l2 = AccessLog(clock=Clock(100), tag="m1"), AccessLog(clock=Clock(200), tag="m2")
    l1.record("a", "write", 1)
    l1.record("a", "read", 1)
    l2.record("a", "read", 9)
    l2.record("b", "pin")
    m = merge_stats(l1, l2)
    assert m["a"].reads == 2 and m["a"].writes == 1
    assert m["a"].size == 9
    assert m["a"].first_seen == l1.stats("a").first_seen
    assert m["a"].last_access == l2.stats("a").last_access
    assert m["b"].pinned


def test_track_access_over_dict_and_files(tmp_path):
    log = AccessLog()
    d = track_access({}, tracker=log)
    d["k"] = b"abc"
    assert d["k"] == b"abc" and d.store == {"k": b"abc"}
    assert (log.stats("k").writes, log.stats("k").reads, log.stats("k").size) == (1, 1, 3)
    del d["k"]
    assert log.stats("k") is None

    root = tmp_path / "files"
    root.mkdir()
    f = track_access(Files(str(root)), tracker=log)
    f["x"] = b"12345"
    assert f["x"] == b"12345"
    assert log.stats("x").reads == 1


def test_default_access_log_beside_not_inside(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    log = default_access_log_for(Files(str(root) + os.sep))
    assert log.path == str(root) + ".access.jsonl"
    with pytest.warns(UserWarning):
        assert default_access_log_for({}).path is None
