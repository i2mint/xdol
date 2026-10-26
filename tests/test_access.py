"""Tests for xdol.access."""

import json
import os
import warnings
from dataclasses import FrozenInstanceError

import pytest
from dol import Files

from xdol import (
    AccessLog,
    AccessStats,
    default_access_log_for,
    merge_stats,
    track_access,
)


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


# -- review fixes ---------------------------------------------------------------


def _lines(path):
    return [json.loads(line) for line in open(path, encoding="utf-8")]


def _write_events(path, *events, raw=b""):
    with open(path, "wb") as fh:
        for ev in events:
            fh.write((json.dumps(ev) + "\n").encode())
        fh.write(raw)


def test_on_disk_event_schema(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=lambda: 7.0, tag="m1")
    log.record("a", "read", 3, served_from="cold")
    log.close()
    assert _lines(p) == [
        {"k": "a", "op": "read", "n": 3, "from": "cold", "ts": 7.0, "tag": "m1"}
    ]


def test_last_read_set_by_reads_only():
    log = AccessLog(clock=Clock(100))
    log.record("a", "write", 1)
    assert log["a"].last_read == 0.0
    log.record("a", "read", 1)
    assert log["a"].last_read == log["a"].last_access == 102


def test_mapping_interface_and_immutable_stats():
    log = AccessLog()
    log.record("a", "read", 1)
    with pytest.raises(KeyError):
        log["missing"]
    assert log.stats("missing") is None and log.get("a") is log.stats("a")
    assert dict(log) == {"a": log["a"]}
    with pytest.raises(FrozenInstanceError):
        log["a"].reads = 999
    assert log["a"].reads == 1


def test_merge_pinned_is_or_and_size_from_latest_access():
    late = AccessLog(clock=Clock(500))
    early = AccessLog(clock=Clock(100))
    late.record("a", "read", 5)
    late.record("a", "pin")
    early.record("a", "read", 9)
    m = merge_stats(late, early)  # the later access comes from the first log
    assert m["a"].pinned and m["a"].size == 5 and m["a"].reads == 2


def test_merge_first_seen_ignores_zeros():
    log = AccessLog(clock=Clock(100))
    log.record("a", "read", 1)
    m = merge_stats({"a": AccessStats(reads=1)}, log)
    assert m["a"].first_seen == 101 and m["a"].reads == 2


def test_unpin_honored_live_and_on_replay(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock())
    log.record("a", "pin")
    log.record("a", "unpin")
    log.close()
    assert not log["a"].pinned and not AccessLog(p)["a"].pinned


def test_replay_delete_release_unpin(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_events(
        p,
        {"k": "a", "op": "write", "n": 2, "ts": 1},
        {"k": "a", "op": "delete", "ts": 2},
        {"k": "b", "op": "write", "n": 4, "ts": 3},
        {"k": "b", "op": "pin", "ts": 4},
        {"k": "b", "op": "release", "ts": 5},
        {"k": "b", "op": "unpin", "ts": 6},
    )
    log = AccessLog(p)
    assert list(log) == ["b"]
    assert log["b"] == AccessStats(size=4, writes=1, first_seen=3, last_access=3)


def test_no_phantom_entries(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_events(p, {"k": "ghost", "op": "open", "ts": 1})
    log = AccessLog(p)
    log.record("gone", "release")
    log.record("gone", "unpin")
    assert len(log) == 0


def test_torn_line_does_not_eat_next_event(tmp_path):
    p = tmp_path / "log.jsonl"
    torn = b'{"k":"a","op":"re'
    _write_events(p, {"k": "a", "op": "read", "n": 1, "ts": 1}, raw=torn)
    log = AccessLog(p)
    log.record("a", "read", 1)
    log.close()
    assert AccessLog(p)["a"].reads == 2


def test_undecodable_bytes_are_skipped(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_events(p, {"k": "a", "op": "read", "n": 1, "ts": 1}, raw=b"\xff\xfe\n")
    assert AccessLog(p)["a"].reads == 1


def test_tracer_line_without_n_keeps_size(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_events(
        p,
        {"k": "f", "op": "write", "n": 1000, "ts": 1},
        {"k": "f", "op": "read", "ts": 2},  # a path-only tracer: no "n"
        {"k": "f", "op": "read"},  # nor "ts"
    )
    st = AccessLog(p)["f"]
    assert (st.size, st.reads, st.writes, st.last_read) == (1000, 2, 1, 2)


def test_out_of_order_timestamps(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_events(
        p,
        {"k": "a", "op": "read", "n": 2, "ts": 200},
        {"k": "a", "op": "read", "n": 1, "ts": 150},
    )
    st = AccessLog(p)["a"]
    assert (st.first_seen, st.last_access, st.last_read, st.size) == (150, 200, 200, 2)


def test_non_string_key_warns_once(tmp_path):
    log = AccessLog(tmp_path / "log.jsonl")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        log.record("ok", "read", 1)
    with pytest.warns(UserWarning, match="JSON round trip"):
        log.record(("a", 1), "read", 1)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        log.record(("b", 2), "read", 1)
    assert log[("a", 1)].reads == 1


def test_track_access_on_non_sized_value():
    inner = {"a": 1}
    s = track_access(inner, tracker=AccessLog())
    assert s["a"] == 1
    s["b"] = 2
    assert inner == {"a": 1, "b": 2}
    assert s.tracker["a"].size == s.tracker["b"].size == 0


def test_track_access_clock_with_tracker_is_an_error():
    with pytest.raises(ValueError):
        track_access({}, tracker=AccessLog(), clock=Clock())


def test_access_log_keeps_identity_semantics():
    a, b = AccessLog(), AccessLog()
    assert a != b and a == a and len({a, b}) == 2


def test_compact_leaves_no_temp_file(tmp_path):
    log = AccessLog(str(tmp_path / "log.jsonl"))
    log.record("a", "read")
    log.compact()
    assert os.listdir(tmp_path) == ["log.jsonl"]
    assert json.loads((tmp_path / "log.jsonl").read_text().splitlines()[0])["k"] == "a"
