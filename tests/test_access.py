"""Tests for xdol.access."""

import itertools
import json
import os
import random
import warnings
from dataclasses import FrozenInstanceError

import pytest
from dol import Files

from xdol.access import _fold_buffered, _fold_streaming, _OutOfOrder

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
    l1, l2 = (
        AccessLog(clock=Clock(100), tag="m1"),
        AccessLog(clock=Clock(200), tag="m2"),
    )
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
    assert (log.stats("k").writes, log.stats("k").reads, log.stats("k").size) == (
        1,
        1,
        3,
    )
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
    assert sorted(os.listdir(tmp_path)) == ["log.jsonl", "log.jsonl.lock"]
    assert json.loads((tmp_path / "log.jsonl").read_text().splitlines()[0])["k"] == "a"


def _write_lines(path, *events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events))


def test_replay_skips_lines_with_wrong_types(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(
        p,
        {"k": "a", "op": "read", "n": 5, "ts": 10.0},
        {"k": "a", "op": "read", "n": "12", "ts": 11.0},
        {"k": "a", "op": "read", "n": 5, "ts": "late"},
        {"k": "b", "op": "snapshot", "stats": {"reads": "many"}},
        {"k": "c", "op": "snapshot", "stats": {"pinned": 1}},
    )
    log = AccessLog(p)
    assert log["a"].reads == 1 and log["a"].size == 5
    assert "b" not in log and "c" not in log


def test_replay_orders_delete_and_pin_by_timestamp(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(
        p,
        {"k": "a", "op": "write", "n": 1, "ts": 1.0},
        {"k": "a", "op": "delete", "ts": 10.0},
        {"k": "a", "op": "read", "n": 1, "ts": 5.0},  # older than the delete: void
        {"k": "b", "op": "write", "n": 1, "ts": 1.0},
        {"k": "b", "op": "read", "n": 1, "ts": 15.0},
        {"k": "b", "op": "delete", "ts": 10.0},  # older than the read: keep b
        {"k": "c", "op": "write", "n": 1, "ts": 1.0},
        {"k": "c", "op": "unpin", "ts": 7.0},
        {"k": "c", "op": "pin", "ts": 3.0},  # older than the unpin: ignored
    )
    log = AccessLog(p)
    assert "a" not in log
    assert "b" in log
    assert not log["c"].pinned


_HISTORY = (  # (op, ts): out of order on purpose
    ("write", 1.0),
    ("pin", 2.0),
    ("read", 15.0),
    ("read", 5.0),
    ("delete", 10.0),
)


def test_replay_in_any_file_order_equals_live_recording(tmp_path):
    clock = Clock()
    live = AccessLog(clock=clock)
    for op, ts in sorted(_HISTORY, key=lambda e: e[1]):
        clock.t = ts - 1  # Clock adds 1 before returning
        live.record("a", op, 3)
    expected = dict(live)
    assert expected["a"].reads == 1 and not expected["a"].pinned
    p = tmp_path / "log.jsonl"
    for order in itertools.permutations(_HISTORY):
        _write_lines(p, *({"k": "a", "op": op, "n": 3, "ts": ts} for op, ts in order))
        assert dict(AccessLog(p)) == expected, order


def test_replay_delete_without_timestamp_still_deletes(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(
        p,
        {"k": "a", "op": "write", "n": 1, "ts": 5.0},
        {"k": "a", "op": "pin", "ts": 6.0},
        {"k": "a", "op": "delete"},
    )
    assert "a" not in AccessLog(p)


def test_replay_skips_non_finite_and_falsy_wrong_typed_numbers(tmp_path):
    p = tmp_path / "log.jsonl"
    good = {"k": "a", "op": "read", "n": 5, "ts": 10.0}
    bad = [
        {**good, "ts": float("nan")},
        {**good, "ts": float("inf")},
        {**good, "n": float("nan")},
        {**good, "n": float("-inf")},
        {**good, "ts": []},
        {**good, "ts": ""},
        {**good, "ts": False},
        {**good, "n": False},
        {**good, "ts": 10**400},
    ]
    p.write_text("".join(json.dumps(e) + "\n" for e in [good, *bad]))  # NaN, Infinity
    log = AccessLog(p)
    assert log["a"].reads == 1 and log["a"].size == 5
    assert log["a"].last_access == 10.0


def test_float_size_survives_compact_and_reopen(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(p, {"k": "a", "op": "write", "n": 3.0, "ts": 1.0})
    log = AccessLog(p)
    assert log["a"].size == 3 and type(log["a"].size) is int
    log.compact()
    assert AccessLog(p)["a"].size == 3


def test_replay_rejects_bool_in_int_field(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(p, {"k": "a", "op": "snapshot", "stats": {"reads": True}})
    assert "a" not in AccessLog(p)


def test_unstamped_line_is_ordered_after_every_earlier_line(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(
        p,
        {"k": "a", "op": "write", "n": 1, "ts": 5.0},
        {"k": "b", "op": "write", "n": 1, "ts": 1.0},
        {"k": "a", "op": "delete"},
    )
    assert "a" not in AccessLog(p) and "b" in AccessLog(p)


def test_record_survives_bad_sizes_and_backwards_clock():
    clock = Clock()
    log = AccessLog(clock=clock)
    log.record("a", "write", float("nan"))
    log.record("a", "write", True)
    assert log["a"].size == 0
    clock.t -= 50  # the clock steps back (NTP): events never go back in time
    log.record("a", "delete")
    assert "a" not in log


def _random_log_lines(rng):
    """Unstamped lines, equal and backwards ts, mid-file snapshots, torn lines."""
    lines, t = [], 0
    for _ in range(rng.randint(0, 25)):
        r = rng.random()
        if r < 0.08:
            lines.append('{"k": "a", "op": "re')
            continue
        if r < 0.15:
            stats = {
                "reads": rng.randint(0, 5),
                "size": rng.randint(0, 9),
                "last_access": float(rng.randint(0, 20)),
            }
            lines.append(json.dumps({"k": rng.choice("abc"), "op": "snapshot", "stats": stats}))
            continue
        if r < 0.2:
            stone = {"k": rng.choice("abc"), "op": "tombstone", "ts": rng.randint(0, 20)}
            lines.append(json.dumps(stone))
            continue
        ev = {
            "k": rng.choice("abc"),
            "op": rng.choice(["read", "write", "delete", "pin", "unpin", "release"]),
            "n": rng.randint(0, 9),
        }
        if rng.random() < 0.8:
            t += rng.choice([0, 0, 1, 2]) if rng.random() < 0.85 else -rng.randint(1, 3)
            # (t may go below 0: negative timestamps exercise the 0.0 running-max floor)
            ev["ts"] = t
        lines.append(json.dumps(ev))
    tail = "\n" if rng.random() < 0.7 else ""
    return ("\n".join(lines) + tail).encode().splitlines(keepends=True)


def test_streaming_and_buffered_replay_agree_fuzz():
    rng = random.Random(0)
    streamed = 0
    for _ in range(3000):
        lines = _random_log_lines(rng)
        buffered, b_tombs = {}, {}
        b_last, b_n = _fold_buffered(lines, buffered, b_tombs)
        streaming, s_tombs = {}, {}
        try:
            s_last, s_n = _fold_streaming(lines, streaming, s_tombs)
        except _OutOfOrder:
            continue
        streamed += 1
        assert (streaming, s_tombs) == (buffered, b_tombs), lines
        assert (s_last, s_n) == (b_last, b_n), lines
    assert streamed > 100  # the fuzz does exercise the streaming path


def test_shuffled_log_gives_same_stats(tmp_path):
    ordered = [
        {"k": "a", "op": "write", "n": 5, "ts": 1},
        {"k": "a", "op": "read", "n": 5, "ts": 2},
        {"k": "b", "op": "read", "n": 7, "ts": 3},
    ]
    p = tmp_path / "ordered.jsonl"
    p.write_text("".join(json.dumps(e) + "\n" for e in ordered))
    q = tmp_path / "shuffled.jsonl"
    q.write_text("".join(json.dumps(e) + "\n" for e in reversed(ordered)))
    assert dict(AccessLog(q)) == dict(AccessLog(p))


def test_snapshot_after_events_still_applied_first(tmp_path):
    p = tmp_path / "cat.jsonl"
    lines = [
        {"k": "a", "op": "read", "n": 1, "ts": 10},
        {"k": "a", "op": "snapshot", "stats": {"reads": 4, "size": 1, "last_access": 5.0}},
    ]
    p.write_text("".join(json.dumps(e) + "\n" for e in lines))
    assert AccessLog(p)["a"].reads == 5


def test_compact_above_threshold(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, tag="t")
    for _ in range(10):
        log.record("a", "read", 3)
    log.close()
    assert len(p.read_text().splitlines()) == 10
    reopened = AccessLog(p, compact_above=5)
    assert len(p.read_text().splitlines()) == 1
    assert reopened["a"].reads == 10
    assert AccessLog(p)["a"].reads == 10


def test_compact_above_counts_events_not_snapshot_lines(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, tag="t")
    for k in range(10):  # more keys than the threshold
        log.record(str(k), "read", 1)
    log.close()
    AccessLog(p, compact_above=5)  # 10 events: compacted to 10 snapshot lines
    assert len(p.read_text().splitlines()) == 10
    before = os.stat(p)
    reopened = AccessLog(p, compact_above=5)  # 0 events: untouched
    after = os.stat(p)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert len(reopened) == 10


def test_compact_above_failure_only_warns(tmp_path, monkeypatch):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, tag="t")
    for _ in range(10):
        log.record("a", "read", 3)
    log.close()

    def fail(self):
        raise OSError("read-only")

    monkeypatch.setattr(AccessLog, "compact", fail)
    with pytest.warns(UserWarning, match="could not compact"):
        reopened = AccessLog(p, compact_above=5)
    assert reopened["a"].reads == 10
    assert len(p.read_text().splitlines()) == 10


@pytest.mark.parametrize("bad", [-1, 1.5, True, "5"])
def test_compact_above_validated(tmp_path, bad):
    with pytest.raises(ValueError):
        AccessLog(tmp_path / "log.jsonl", compact_above=bad)


def test_snapshot_without_admitted_at_loads(tmp_path):
    p = tmp_path / "old.jsonl"
    old = {"reads": 3, "size": 2, "last_access": 5.0, "pinned": True}
    p.write_text(json.dumps({"k": "a", "op": "snapshot", "stats": old}) + "\n")
    st = AccessLog(p)["a"]
    assert st.reads == 3 and st.pinned and st.admitted_at == 0.0


def test_merge_stats_admitted_at_from_latest_access():
    early = {"a": AccessStats(last_access=10.0, admitted_at=8.0)}
    late = {"a": AccessStats(last_access=20.0, admitted_at=0.0)}  # since released
    assert merge_stats(early, late)["a"].admitted_at == 0.0
    assert merge_stats(late, early)["a"].admitted_at == 0.0
    late = {"a": AccessStats(last_access=20.0, admitted_at=15.0)}
    assert merge_stats(late, early)["a"].admitted_at == 15.0


def test_old_delete_does_not_wipe_a_newer_snapshot(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(
        p,
        {"k": "a", "op": "snapshot", "stats": {"reads": 4, "size": 1, "last_access": 50.0}},
        {"k": "a", "op": "delete", "ts": 10.0},
        {"k": "b", "op": "snapshot", "stats": {"reads": 1, "last_access": 5.0}},
        {"k": "b", "op": "delete", "ts": 5.0},  # same stamp: the delete wins
    )
    log = AccessLog(p)
    assert log["a"].reads == 4 and "b" not in log


def test_snapshot_with_float_size_loads(tmp_path):
    p = tmp_path / "old.jsonl"
    _write_lines(p, {"k": "a", "op": "snapshot", "stats": {"reads": 2, "size": 7.0}})
    st = AccessLog(p)["a"]
    assert st.size == 7 and type(st.size) is int and st.reads == 2


def test_delete_after_reopen_with_clock_behind_snapshot_still_deletes(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(p, {"k": "a", "op": "snapshot", "stats": {"pinned": True, "last_access": 100.0}})
    log = AccessLog(p, clock=lambda: 90.0)
    log.record("a", "delete")
    assert "a" not in log and "a" not in AccessLog(p)


def test_non_integral_float_size_snapshot_is_rejected(tmp_path):
    p = tmp_path / "old.jsonl"
    _write_lines(p, {"k": "a", "op": "snapshot", "stats": {"size": 7.5}})
    assert "a" not in AccessLog(p)


def test_compact_keeps_other_writers_events(tmp_path):
    # the #23 repro: A compacting must not drop B's events, nor B's later appends
    p = tmp_path / "log.jsonl"
    a = AccessLog(p, clock=Clock(), compact_above=None)
    b = AccessLog(p, clock=Clock(), compact_above=None)
    for _ in range(5):
        b.record("k", "read", 1)
    a.compact()
    b.record("k", "read", 1)
    a.record("k", "read", 1)
    assert AccessLog(p)["k"].reads == 7
    assert b["k"].reads == 6 and a["k"].reads == 1  # in-memory views are untouched


def test_compact_without_file_does_nothing(tmp_path):
    p = tmp_path / "log.jsonl"
    AccessLog(p).compact()
    assert not p.exists()


def test_compaction_while_recording(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock(), compact_above=5)
    for _ in range(12):
        log.record("a", "read", 1)
    assert len(p.read_text().splitlines()) <= 6
    assert AccessLog(p)["a"].reads == 12 == log["a"].reads


def test_compact_above_defaults_on(tmp_path):
    from xdol.access import DFLT_COMPACT_ABOVE

    assert AccessLog(tmp_path / "log.jsonl").compact_above == DFLT_COMPACT_ABOVE


def test_tombstones_survive_compaction_and_reopen(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock())
    log.record("a", "write", 1)
    log.record("a", "delete")
    log.record("b", "write", 1)
    log.record("b", "delete")
    log.record("b", "write", 2)  # re-created: its tombstone stays
    stones = dict(log.tombstones)
    assert set(stones) == {"a", "b"}
    for again in (AccessLog(p), (log.compact(), AccessLog(p))[1]):
        assert dict(again.tombstones) == stones
        assert "a" not in again and again["b"].writes == 1


def test_tombstones_are_read_only():
    with pytest.raises(TypeError):
        AccessLog().tombstones["a"] = 1.0


def test_merge_stats_honours_tombstones():
    deleter = AccessLog(clock=lambda: 10.0)
    deleter.record("a", "delete")
    before = {"a": AccessStats(reads=3, last_access=10.0)}  # same stamp: delete wins
    after = {"a": AccessStats(reads=2, last_access=11.0)}
    assert "a" not in merge_stats(before, deleter)
    assert "a" not in merge_stats(deleter, before)
    assert merge_stats(before, after, deleter)["a"].reads == 2


def _append_reads(path, n, key, start):
    start.wait(60)  # all processes append (and compact) at once
    log = AccessLog(path, compact_above=1)  # compacts every other append
    for _ in range(n):
        log.record(key, "read", 1)
    log.close()


def test_concurrent_processes_with_compaction_lose_nothing(tmp_path):
    import multiprocessing

    p = str(tmp_path / "log.jsonl")
    ctx = multiprocessing.get_context("spawn")
    start = ctx.Event()
    procs = [
        ctx.Process(target=_append_reads, args=(p, 300, key, start)) for key in "aab"
    ]
    for proc in procs:
        proc.start()
    start.set()
    for proc in procs:
        proc.join(120)
        assert proc.exitcode == 0
    log = AccessLog(p, compact_above=None)
    assert (log["a"].reads, log["b"].reads) == (600, 300)
    assert len(open(p).read().splitlines()) < 10  # compaction did run


# -- tombstone lines, merge rule, fork, lock fallback ----------------------------


def test_recreated_at_delete_stamp_survives_compaction(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=lambda: 10.0)
    log.record("k", "write", 3)
    log.record("k", "delete")
    log.record("k", "write", 5)  # same tick as the delete
    log.compact()
    again = AccessLog(p)
    assert again["k"].writes == 1 and again["k"].size == 5
    assert dict(again.tombstones) == {"k": 10.0}
    assert "k" in merge_stats(again) and "k" in merge_stats(log)


def test_pin_after_delete_survives_compaction(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock())
    log.record("k", "write", 3)
    log.record("k", "delete")
    log.record("k", "pin")  # re-created by pin: last_access stays 0
    log.compact()
    again = AccessLog(p)
    assert again["k"].pinned and "k" in again.tombstones


def test_tombstones_are_not_events(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock(), compact_above=50)
    for i in range(200):
        log.record(f"tmp{i}", "write", 1)
        log.record(f"tmp{i}", "delete")
    log.close()
    log.compact()
    assert {e["op"] for e in _lines(p)} == {"tombstone"}
    before = os.stat(p)
    for _ in range(2):
        again = AccessLog(p, compact_above=50)
        again.close()
    after = os.stat(p)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert len(again) == 0 and len(again.tombstones) == 200


def test_record_rejects_tombstone_op():
    with pytest.raises(ValueError):
        AccessLog().record("a", "tombstone")


def test_tombstone_after_event_is_out_of_order():
    lines = [
        json.dumps({"k": "a", "op": "write", "n": 1, "ts": 1.0}).encode(),
        json.dumps({"k": "a", "op": "tombstone", "ts": 5.0}).encode(),
    ]
    with pytest.raises(_OutOfOrder):
        _fold_streaming(lines, {}, {})
    stats, stones = {}, {}
    _, nevents = _fold_buffered(lines, stats, stones)
    assert nevents == 1 and "a" not in stats and stones == {"a": 5.0}  # wiped


def test_merge_stats_own_tombstones_do_not_apply():
    log = AccessLog(clock=Clock())
    log.record("k", "write", 1)
    log.record("k", "delete")
    log.record("k", "pin")  # first_seen after the delete, last_access 0
    assert merge_stats(log)["k"].pinned


def test_merge_stats_other_logs_tombstones_apply():
    a = AccessLog(clock=lambda: 10.0)
    a.record("k", "delete")
    older = {"k": AccessStats(reads=2, first_seen=1.0, last_access=5.0)}
    newer = {"k": AccessStats(reads=3, first_seen=1.0, last_access=15.0)}
    assert "k" not in merge_stats(a, older)
    assert merge_stats(a, newer)["k"].reads == 3


def test_access_log_is_a_context_manager(tmp_path):
    with AccessLog(tmp_path / "log.jsonl") as log:
        log.record("a", "read", 1)
        assert log._appender._fh is not None or os.name == "nt"
    assert log._appender._fh is None and log._appender._lock_fh is None


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded")
def test_forked_children_appending_while_parent_compacts_lose_nothing(tmp_path):
    p = str(tmp_path / "log.jsonl")
    AccessLog(p).record("seed", "read", 1)
    log = AccessLog(p, compact_above=None)  # its lock handle is inherited by the kids
    kids = []
    for _ in range(4):
        pid = os.fork()
        if pid == 0:  # pragma: no cover (child)
            code = 1
            try:
                for _ in range(300):
                    log.record("k", "read", 1)
                code = 0
            finally:
                os._exit(code)
        kids.append(pid)
    for _ in range(100):
        log.compact()
    for pid in kids:
        assert os.waitpid(pid, 0)[1] == 0
    assert AccessLog(p, compact_above=None)["k"].reads == 1200


@pytest.mark.skipif(
    os.name == "nt" or os.geteuid() == 0, reason="POSIX permissions, not root"
)
def test_read_only_lock_file_still_locks(tmp_path):
    import fcntl

    p = tmp_path / "log.jsonl"
    lock = tmp_path / "log.jsonl.lock"
    lock.touch()
    lock.chmod(0o444)
    log = AccessLog(p)
    with log._appender.locked(), open(lock, "rb") as other:
        with pytest.raises(BlockingIOError):
            fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    log.record("a", "read", 1)
    assert AccessLog(p)["a"].reads == 1


def _fold_keys(lines):
    stats, stones = {}, {}
    _fold_buffered(lines, stats, stones)
    return stats


def test_compaction_does_not_change_the_fold_fuzz():
    # compacting a prefix changes nothing when the events after it are stamped after
    # it (a late event stamped before compacted ones folds approximately, see the module docstring: #44)
    from xdol.access import _compacted_lines

    rng = random.Random(1)
    for _ in range(3000):
        stamps = rng.sample(range(1, 60), rng.randint(2, 25))  # distinct: no ties
        events = [
            {
                "k": rng.choice("ab"),
                "op": rng.choice(["read", "write", "delete", "pin", "unpin"]),
                "n": rng.randint(0, 9),
                "ts": float(t),
            }
            for t in stamps
        ]
        lines = [json.dumps(e).encode() for e in events]
        cut = rng.randint(0, len(lines))
        prefix_stats, prefix_stones = {}, {}
        _fold_buffered(lines[:cut], prefix_stats, prefix_stones)
        compacted = list(_compacted_lines(prefix_stats, prefix_stones))
        raw, comp = _fold_keys(lines), _fold_keys(compacted + lines[cut:])
        if cut and cut < len(events) and min(stamps[cut:]) < max(stamps[:cut]):
            continue  # a late event: see #44
        assert raw.keys() == comp.keys(), events
        for k in raw:
            assert (raw[k].reads, raw[k].writes, raw[k].pinned) == (
                comp[k].reads,
                comp[k].writes,
                comp[k].pinned,
            ), events


def test_late_event_older_than_a_compacted_delete_stays_deleted(tmp_path):
    p = tmp_path / "log.jsonl"
    _write_lines(
        p,
        {"k": "k", "op": "write", "n": 1, "ts": 10.0},
        {"k": "k", "op": "delete", "ts": 20.0},
    )
    AccessLog(p).compact()
    with open(p, "a") as fh:  # a slow writer's event, stamped before the delete
        fh.write(json.dumps({"k": "k", "op": "read", "n": 1, "ts": 15.0}) + "\n")
    assert "k" not in AccessLog(p)


def test_unstamped_delete_after_compaction_still_deletes(tmp_path):
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock())
    log.record("k", "write", 1)
    log.compact()
    with open(p, "a") as fh:  # a tracer that writes no timestamp
        fh.write(json.dumps({"k": "k", "op": "delete"}) + "\n")
    assert "k" not in AccessLog(p)


def test_pathological_line_never_raises_into_reads(tmp_path):
    p = tmp_path / "log.jsonl"
    p.write_text("[" * 200_000 + "\n")
    log = AccessLog(p, compact_above=0)
    store = track_access({"a": b"x"}, tracker=log)
    assert store["a"] == b"x" and log["a"].reads == 1


def test_no_compaction_without_the_lock(tmp_path, monkeypatch):
    import errno

    import xdol.access

    def unsupported(fh):
        raise OSError(errno.ENOTSUP, "flock not supported")

    monkeypatch.setattr(xdol.access, "_lock", unsupported)
    p = tmp_path / "log.jsonl"
    log = AccessLog(p, clock=Clock(), compact_above=2)
    with pytest.warns(UserWarning, match="could not compact"):
        for _ in range(4):
            log.record("a", "read", 1)  # appended unlocked, never compacted
    assert len(p.read_text().splitlines()) == 4
    assert AccessLog(p, compact_above=None)["a"].reads == 4
    with pytest.raises(OSError, match="not compacting"):
        log.compact()
