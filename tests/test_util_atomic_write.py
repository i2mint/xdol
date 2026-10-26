"""Tests for ``xdol.util.atomic_write``."""

import os

import pytest

from xdol.util import atomic_write


def test_writes_bytes_and_str(tmp_path):
    p = tmp_path / "f"
    atomic_write(p, b"\x00\x01")
    assert p.read_bytes() == b"\x00\x01"
    atomic_write(p, "héllo")
    assert p.read_text(encoding="utf-8") == "héllo"
    assert os.listdir(tmp_path) == ["f"]


def test_failure_leaves_old_content_and_no_temp(tmp_path):
    p = tmp_path / "f"
    atomic_write(p, b"old")
    with pytest.raises(TypeError):
        atomic_write(p, 123)  # type: ignore[arg-type]
    assert p.read_bytes() == b"old"
    assert os.listdir(tmp_path) == ["f"]


def test_tmp_suffix_is_used(tmp_path, monkeypatch):
    seen = []
    real = os.replace
    monkeypatch.setattr(os, "replace", lambda a, b: (seen.append(a), real(a, b)))
    atomic_write(tmp_path / "f", b"x", tmp_suffix=".mine")
    assert seen[0].endswith(".mine")
