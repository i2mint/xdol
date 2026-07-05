"""Regression tests for dol Issue #18 in xdol's py stores.

Methods defined on a ``wrap_kvs``/``filt_iter``/relative-path-wrapped class run with
``self`` bound to the inner, unwrapped store, so key relativization and key filtering are
bypassed. The fix routes those internal accesses through ``dol.wrapped_self(self)`` (the
outer, transform-applying store). These tests pin the corrected behavior.
"""

from xdol.pystores import PyFilesReader, SetupCfgReader, PyprojectReader


def test_is_pkg_and_init_file_contents(tmp_path):
    pkg = tmp_path / "mypkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("VERSION = '1.0'\n")
    (pkg / "mod.py").write_text("x = 1\n")

    reader = PyFilesReader(str(pkg))
    # is_pkg reads the relativized outer key '__init__.py' (was False under Issue #18)
    assert reader.is_pkg() is True
    assert reader.init_file_contents() == "VERSION = '1.0'\n"

    nonpkg = tmp_path / "nopkg"
    nonpkg.mkdir()
    (nonpkg / "mod.py").write_text("y = 2\n")
    reader2 = PyFilesReader(str(nonpkg))
    assert reader2.is_pkg() is False
    assert reader2.init_file_contents() is None


def test_setupcfg_reader_honors_filter_with_decoy(tmp_path):
    proj = tmp_path / "proj1"
    proj.mkdir()
    (proj / "setup.cfg").write_text("[options]\ninstall_requires = requests")
    # A decoy .cfg that is NOT setup.cfg but would yield a dependency if the filter leaked.
    (proj / "other.cfg").write_text("[options]\ninstall_requires = SHOULD_NOT_APPEAR")

    reader = SetupCfgReader(str(tmp_path))
    deps = list(reader.dependencies_from_all())
    assert "requests" in deps
    assert "SHOULD_NOT_APPEAR" not in deps  # decoy leaked under Issue #18


def test_pyproject_reader_honors_filter_with_decoy(tmp_path):
    proj = tmp_path / "proj1"
    proj.mkdir()
    (proj / "pyproject.toml").write_text('[project]\ndependencies = ["requests>=2.0"]')
    # A decoy .toml whose basename != pyproject.toml; must not leak into results.
    (proj / "decoy.toml").write_text('[project]\ndependencies = ["DECOY"]')

    reader = PyprojectReader(str(tmp_path))
    deps = list(reader.dependencies_from_all())
    assert any(d.startswith("requests") for d in deps)
    assert "DECOY" not in deps  # decoy leaked under Issue #18
