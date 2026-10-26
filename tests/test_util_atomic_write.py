"""Tests for ``xdol.util.atomic_write``."""

import os

import pytest

from xdol.util import atomic_write


def test_writes_bytes_and_chunks(tmp_path):
    p = tmp_path / "f"
    atomic_write(p, b"\x00\x01")
    assert p.read_bytes() == b"\x00\x01"
    atomic_write(p, (c for c in [b"a", b"b"]))
    assert p.read_bytes() == b"ab"
    assert os.listdir(tmp_path) == ["f"]


def test_str_is_rejected(tmp_path):
    with pytest.raises(TypeError):
        atomic_write(tmp_path / "f", 123)  # type: ignore[arg-type]
    assert os.listdir(tmp_path) == []


def test_failure_midway_leaves_old_content_and_no_temp(tmp_path):
    p = tmp_path / "f"
    atomic_write(p, b"old")

    def chunks():
        yield b"new"
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        atomic_write(p, chunks())
    assert p.read_bytes() == b"old"
    assert os.listdir(tmp_path) == ["f"]


def test_replace_failure_cleans_temp(tmp_path):
    (tmp_path / "d").mkdir()
    with pytest.raises(OSError):
        atomic_write(tmp_path / "d", b"x")  # target is a directory
    assert os.listdir(tmp_path) == ["d"]


def test_tmp_suffix_is_used(tmp_path, monkeypatch):
    seen = []
    real = os.replace
    monkeypatch.setattr(os, "replace", lambda a, b: (seen.append(a), real(a, b)))
    atomic_write(tmp_path / "f", b"x", tmp_suffix=".mine")
    assert seen[0].endswith(".mine")
