"""Smoke test: importing the package shouldn't blow up."""


def test_import():
    import xdol  # noqa: F401
