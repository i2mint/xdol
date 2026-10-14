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


# --- regressions from the adversarial review of PR #9 ---


def test_dict_keys_of_any_type_are_stable_or_loud():
    k = robust_key({frozenset({"alpha", "beta", "gamma", "delta"}): 1, (1, 2): 3})
    code = (
        "from xdol.keys import robust_key;"
        "print(robust_key({frozenset({'alpha','beta','gamma','delta'}): 1, (1, 2): 3}))"
    )
    for seed in (1, 2, 3):
        out = subprocess.run(
            [sys.executable, "-c", code], env={"PYTHONHASHSEED": str(seed), "PATH": ""},
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        assert out == k

    class Foo:
        pass

    with pytest.raises(UnstableKeyError):
        robust_key({Foo(): 1})


def test_colliding_dict_keys_raise_instead_of_dropping_a_value():
    with pytest.raises(UnstableKeyError, match="collide"):
        robust_key({1: "a", "1": "b"})
    assert robust_key({1: "a"}) == robust_key({"1": "a"})  # plain JSON semantics


def test_enum_subclasses_of_primitives_match_their_values():
    class N(enum.IntEnum):
        A = 5

    class S(str, enum.Enum):
        A = "x"

    assert robust_key(N.A) == robust_key(5)
    assert robust_key(S.A) == robust_key("x")


def test_lambdas_and_bound_methods_and_partials():
    with pytest.raises(UnstableKeyError, match="lambda"):
        robust_key(lambda x: x)

    class M:
        def __init__(self, i):
            self.i = i

        def m(self):
            ...

        def __cache_key__(self):
            return self.i

    assert robust_key(M(1).m) != robust_key(M(2).m)
    assert robust_key(M(1).m) == robust_key(M(1).m)

    import functools

    class Plain:
        def go(self):
            ...

    with pytest.raises(UnstableKeyError):  # the address is inside partial's repr
        robust_key(functools.partial(Plain().go))


def test_address_inside_a_repr_is_caught():
    class Wrapper:
        def __repr__(self):
            return "Wrapper(<Foo object at 0x7f12abcd>)"

    with pytest.raises(UnstableKeyError):
        robust_key(Wrapper())


def test_class_with_cache_key_can_itself_be_an_argument():
    class K:
        def __cache_key__(self):
            return 1

    assert robust_key(K) != robust_key(1)


def test_pandas_content_is_fully_keyed():
    pd = pytest.importorskip("pandas")
    a = pd.Series([1, 2], index=["x", "y"])
    b = pd.Series([1, 2], index=["p", "q"])
    assert robust_key(a) != robust_key(b)
    big1 = pd.DataFrame({"v": range(1000)})
    big2 = big1.copy()
    big2.loc[500, "v"] = -1
    assert robust_key(big1) != robust_key(big2)


def test_drop_names_entries_of_var_keyword():
    def h(a, **kw):
        ...

    k = make_key_func(h, drop=["logger"])
    assert k(1, logger=print, x=2) == k(1, x=2) != k(1, x=3)
