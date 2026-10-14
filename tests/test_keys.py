"""Tests for xdol.keys: stable, robust cache keys."""

import dataclasses
import enum
import pathlib
import subprocess
import sys

import pytest

from xdol.keys import UnstableKeyError, make_key_func, normalize, robust_key


def test_golden_keys_match_the_reelee_prototype():
    # Committed cassettes are keyed by these hashes: they must never change.
    assert robust_key("hi") == "010aba4ec5d37d423d20c0993ed5f6cf"
    assert (
        robust_key({"b": [1, 2.5, None, True], "a": " x\n"}, n=3)
        == "23d9d5d61bea35ad094931e26c0187c3"
    )
    assert (
        robust_key(prompt="é  ü", opts={"z": 1, "a": {"q": ["r "]}})
        == "999399648822debdaf3cfb483675ef0e"
    )


def test_superficial_differences_do_not_matter():
    assert robust_key({"a": 1, "b": 2}, p=" x ") == robust_key({"b": 2, "a": 1}, p="x")
    assert robust_key([1, 2]) == robust_key((1, 2))


def test_semantic_differences_matter():
    assert robust_key(1) != robust_key("1")
    assert robust_key("a b") != robust_key("ab")
    assert robust_key([1, 2]) != robust_key([2, 1])
    assert robust_key(x=1) != robust_key(1)


def test_collapse_whitespace_is_opt_in():
    assert normalize("a   b") == "a   b"
    assert normalize(" a \n b ", collapse_whitespace=True) == "a b"


class Color(enum.Enum):
    RED = 1


@dataclasses.dataclass
class Point:
    x: int
    y: int


def test_long_tail_types_are_stable_and_distinct():
    values = [
        b"abc", {1, 2}, frozenset({"a"}), pathlib.Path("/a/b"), Color.RED,
        Point(1, 2), len,
    ]
    keys = [robust_key(v) for v in values]
    assert len(set(keys)) == len(values)
    assert robust_key({3, 1, 2}) == robust_key({2, 3, 1})
    assert robust_key(Point(1, 2)) == robust_key(Point(1, 2))
    assert robust_key(Point(1, 2)) != robust_key(Point(2, 1))


def test_keys_are_stable_across_processes():
    code = (
        "from xdol.keys import robust_key; import pathlib;"
        "print(robust_key({3,1,2}, b'x', pathlib.Path('a'), {'k': {'z', 'y'}}))"
    )
    outs = {
        subprocess.run(
            [sys.executable, "-c", code],
            env={"PYTHONHASHSEED": str(seed), "PATH": ""},
            capture_output=True, text=True, check=True,
        ).stdout
        for seed in (0, 1, 2)
    }
    assert len(outs) == 1


def test_object_with_address_repr_raises_instead_of_hashing_the_address():
    class Opaque:
        pass

    with pytest.raises(UnstableKeyError, match="memory address"):
        robust_key(Opaque())
    with pytest.warns(UserWarning):
        make_key_func(on_unstable="warn")(Opaque())
    make_key_func(on_unstable="ignore")(Opaque())  # accepted, by request


def test_cache_key_protocol():
    class Doc:
        def __init__(self, ident, junk):
            self.ident, self.junk = ident, junk

        def __cache_key__(self):
            return {"doc": self.ident}

    assert robust_key(Doc(1, "a")) == robust_key(Doc(1, "b")) != robust_key(Doc(2, "a"))


def test_make_key_func_binds_signature_and_drops():
    def f(a, b=1, *, log=None):
        ...

    key = make_key_func(f, drop=["log"])
    assert key(1, 2) == key(1, b=2) == key(a=1, b=2, log=print)
    assert key(1, 2) != key(1, 3)
    # defaults only count when asked
    assert key(1) != key(1, 1)
    assert make_key_func(f, drop=["log"], apply_defaults=True)(1) == key(1, 1)


def test_make_key_func_validation_and_formatting():
    with pytest.raises(ValueError, match="needs `func`"):
        make_key_func(drop=["x"])
    with pytest.raises(ValueError, match="no parameter"):
        make_key_func(lambda a: a, drop=["nope"])
    k = make_key_func(prefix="v2-", length=8, digest="md5")("x")
    assert k.startswith("v2-") and len(k) == 11
    with pytest.raises(TypeError):  # signature mismatch surfaces immediately
        make_key_func(lambda a: a)(1, 2)
