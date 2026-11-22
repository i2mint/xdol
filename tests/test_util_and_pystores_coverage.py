"""Coverage-pass tests for ``xdol.util`` and ``xdol.pystores`` (error paths, edge cases)."""

import os
import pickle

import pytest

import xdol
from xdol.pystores import PyprojectReader, SetupCfgReader
from xdol.util import resolve_module_filepath, resolve_to_folder, save_obj


# --- util: resolve_module_filepath / resolve_to_folder -----------------------------------


def test_resolve_module_filepath_from_module_string_and_callable():
    init = os.path.abspath(xdol.__file__)
    assert resolve_module_filepath(xdol) == init
    assert resolve_module_filepath(init) == init
    assert resolve_module_filepath(save_obj).endswith("util.py")


def test_resolve_module_filepath_strips_pyc_suffix(tmp_path):
    src = tmp_path / "m.py"
    src.write_text("")
    assert resolve_module_filepath(str(src) + "c") == str(src)


def test_resolve_module_filepath_of_a_package_dir_gives_its_init(tmp_path):
    (tmp_path / "__init__.py").write_text("")
    assert resolve_module_filepath(str(tmp_path)) == str(tmp_path / "__init__.py")


def test_resolve_module_filepath_of_a_non_package_dir_asserts(tmp_path):
    with pytest.raises(AssertionError, match="wasn't a package"):
        resolve_module_filepath(str(tmp_path))


def test_resolve_module_filepath_missing_file_asserts_unless_told_not_to(tmp_path):
    missing = str(tmp_path / "nope.py")
    with pytest.raises(AssertionError):
        resolve_module_filepath(missing)
    assert (
        resolve_module_filepath(missing, assert_output_is_existing_filepath=False)
        == missing
    )


def test_resolve_to_folder_variants(tmp_path):
    pkg_dir = os.path.dirname(os.path.abspath(xdol.__file__))
    assert resolve_to_folder(xdol) == pkg_dir
    assert resolve_to_folder(pkg_dir) == pkg_dir
    assert resolve_to_folder(os.path.join(pkg_dir, "__init__.py")) == pkg_dir
    # a callable resolves to its source *file*, which is not a folder (only packages are)
    with pytest.raises(AssertionError):
        resolve_to_folder(resolve_to_folder)
    with pytest.raises(AssertionError):
        resolve_to_folder(str(tmp_path / "missing"))
    assert resolve_to_folder(str(tmp_path / "missing"), False) == str(
        tmp_path / "missing"
    )


# --- util: save_obj ------------------------------------------------------------------------


def test_save_obj_into_a_mapping_returns_the_key():
    store = {}
    key = save_obj({"a": 1}, save_under_key=store)
    assert pickle.loads(store[key]) == {"a": 1}


def test_save_obj_with_callable_sink_custom_key_and_return_func():
    seen = []
    out = save_obj(
        [1, 2],
        encode=lambda o: repr(o).encode(),
        key=lambda obj, enc: "k",
        save_under_key=lambda k, v: seen.append((k, v)),
        return_func=lambda obj, enc, k: (obj, k),
    )
    assert out == ([1, 2], "k") and seen == [("k", b"[1, 2]")]


def test_save_obj_rejects_a_sink_that_is_neither_callable_nor_settable():
    with pytest.raises(TypeError, match="save_under_key"):
        save_obj(1, save_under_key=42)


# --- pystores: dependency extraction and decode errors --------------------------------------


def _tree(tmp_path, name, content: bytes):
    d = tmp_path / "proj"
    d.mkdir(exist_ok=True)
    (d / name).write_bytes(content)
    return str(tmp_path)


def test_setup_cfg_dependencies_skip_comments_and_blank_lines(tmp_path):
    root = _tree(
        tmp_path,
        "setup.cfg",
        b"[options]\ninstall_requires =\n    requests\n    # comment\n\n    numpy>=1\n",
    )
    assert list(SetupCfgReader(root).dependencies_from_all()) == [
        "requests",
        "numpy>=1",
    ]


def test_setup_cfg_undecodable_content_reads_as_empty(tmp_path):
    root = _tree(tmp_path, "setup.cfg", b"\xff\xfe\x00bad")
    reader = SetupCfgReader(root)
    (key,) = list(reader)
    assert reader[key] == ""
    assert list(reader.dependencies_from_all()) == []


def test_pyproject_dependencies_include_build_requires(tmp_path):
    root = _tree(
        tmp_path,
        "pyproject.toml",
        b'[build-system]\nrequires = ["setuptools", " "]\n[project]\nname = "x"\ndependencies = ["requests>=2", 3]\n',
    )
    deps = list(PyprojectReader(root).dependencies_from_all())
    assert deps == ["requests>=2", "setuptools"]


def test_pyproject_malformed_toml_is_skipped_not_raised(tmp_path):
    root = _tree(tmp_path, "pyproject.toml", b"[project\nnot toml")
    assert list(PyprojectReader(root).dependencies_from_all()) == []


def test_pyproject_undecodable_content_reads_as_empty(tmp_path):
    root = _tree(tmp_path, "pyproject.toml", b"\xff\xfe\x00bad")
    reader = PyprojectReader(root)
    (key,) = list(reader)
    assert reader[key] == ""
