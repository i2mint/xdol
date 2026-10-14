"""Tests for the codecs and the paired (.json + .bin) store in xdol.x_codecs."""

import pytest
from dol import Files

from xdol.x_codecs import (
    BYTES_CODEC, CODECS, JSON_CODEC, Codec, get_codec, json_codec, paired_store,
    with_codec,
)


def test_get_codec_forms():
    assert get_codec("json") is JSON_CODEC
    assert get_codec(BYTES_CODEC) is BYTES_CODEC
    assert get_codec((str.encode, bytes.decode)).decode(b"a") == "a"
    with pytest.raises(KeyError, match="Unknown codec"):
        get_codec("nope")


def test_json_codec_is_strict_unless_told():
    with pytest.raises(TypeError):
        JSON_CODEC.encode({"x": object()})
    assert json_codec(default=str).decode(json_codec(default=str).encode({"x": 1j})) == {"x": "1j"}


def test_registered_codec_is_usable_by_name():
    CODECS["upper"] = Codec(lambda s: s.upper().encode(), lambda b: b.decode().lower())
    try:
        raw = {}
        s = with_codec(raw, "upper")
        s["k"] = "abc"
        assert raw == {"k": b"ABC"} and s["k"] == "abc"
    finally:
        del CODECS["upper"]


@pytest.fixture(params=["dict", "files"])
def raw(request, tmp_path):
    return {} if request.param == "dict" else Files(str(tmp_path))


def test_paired_store_roundtrip_and_layout(raw):
    p = paired_store(raw)
    p["k"] = ({"prompt": "cat", "n": 1}, b"\x89PNG")
    assert sorted(raw) == ["k.bin", "k.json"]
    assert p["k"] == ({"prompt": "cat", "n": 1}, b"\x89PNG")
    assert "k" in p and list(p) == ["k"] and len(p) == 1
    del p["k"]
    assert list(raw) == [] and "k" not in p
    with pytest.raises(KeyError):
        p["k"]
    with pytest.raises(KeyError):
        del p["k"]


def test_half_written_entry_is_invisible(raw):
    p = paired_store(raw)
    raw["x.bin"] = b"asset only"  # crash before the json "commit marker"
    assert "x" not in p and list(p) == []
    raw["y.json"] = b"{}"  # metadata whose asset was lost
    assert "y" not in p and list(p) == []


def test_paired_store_rejects_non_pairs_and_unencodable_meta_without_writing():
    raw = {}
    p = paired_store(raw)
    with pytest.raises(TypeError, match="pairs"):
        p["k"] = "not a pair"
    with pytest.raises(TypeError):
        p["k"] = ({"x": object()}, b"a")
    assert raw == {}


# --- regressions from the adversarial review of PR #9 ---


def test_bytes_codec_refuses_non_bytes():
    with pytest.raises(TypeError):
        BYTES_CODEC.encode(5)
    p = paired_store({})
    with pytest.raises(TypeError):
        p["k"] = ({}, 5)


def test_json_codec_handles_mixed_key_types():
    assert JSON_CODEC.decode(JSON_CODEC.encode({1: "a", "b": 2})) == {"1": "a", "b": 2}


def test_overwrite_that_fails_midway_leaves_no_valid_mixed_pair():
    class FailsOnJson(dict):
        fail = False

        def __setitem__(self, k, v):
            if self.fail and k.endswith(".json"):
                raise OSError("disk full")
            super().__setitem__(k, v)

    raw = FailsOnJson()
    p = paired_store(raw)
    p["k"] = ({"prompt": "cat"}, b"CAT")
    raw.fail = True
    with pytest.raises(OSError):
        p["k"] = ({"prompt": "dog"}, b"DOG")
    assert "k" not in p  # absent, not a cat/dog chimera


def test_suffixes_are_validated():
    with pytest.raises(ValueError):
        paired_store({}, meta_suffix="")
    with pytest.raises(ValueError):
        paired_store({}, meta_suffix=".x", asset_suffix=".x")


def test_overlapping_suffixes_rejected():
    with pytest.raises(ValueError, match="overlap"):
        paired_store({}, meta_suffix=".json", asset_suffix=".x.json")


def test_decode_failures_are_codec_decode_errors():
    from xdol.x_codecs import CodecDecodeError

    s = with_codec({"k": b"{oops"}, "json")
    with pytest.raises(CodecDecodeError):
        s["k"]
    with pytest.raises(KeyError):  # a missing key stays a plain KeyError
        s["absent"]


def test_paired_store_detects_a_torn_pair():
    from xdol.x_codecs import CodecDecodeError

    raw = {}
    p = paired_store(raw)
    p["k"] = ({"seed": "A"}, b"A")
    raw["k.bin"] = b"B"
    with pytest.raises(CodecDecodeError, match="belong together"):
        p["k"]


def test_legacy_bare_metadata_layout_is_still_read():
    raw = {"k.json": b'{"prompt": "cat"}', "k.bin": b"A"}
    assert paired_store(raw)["k"] == ({"prompt": "cat"}, b"A")
