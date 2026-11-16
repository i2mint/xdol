"""The CI replay path: the same memoized function over a filesystem or an S3 store.

Uses s3dol's in-memory fake (no network, no credentials); skipped without s3dol.
"""

import pytest

pytest.importorskip("s3dol")
pytest.importorskip("boto3")

from s3dol.testing import mock_s3  # noqa: E402

from xdol.memoize import CacheMiss, memoize_calls  # noqa: E402


def test_record_to_filesystem_then_replay_from_s3(tmp_path):
    calls = []

    def costly(prompt):
        calls.append(prompt)
        return {"answer": prompt.upper()}

    # Record locally...
    memoize_calls(costly, store=tmp_path, mode="record")("hi")
    assert calls == ["hi"]

    # ...publish the cassette files to a bucket (here: the in-memory fake)...
    bucket = mock_s3()
    for path in tmp_path.iterdir():
        bucket[path.name] = path.read_bytes()

    # ...and replay from it in CI: same call site, a bytes store + codec.
    replay = memoize_calls(costly, store=bucket, codec="json")
    assert replay("hi") == {"answer": "HI"}
    assert calls == ["hi"]
    with pytest.raises(CacheMiss):
        replay("something new")


def test_record_into_s3_paired_assets():
    bucket = mock_s3()

    def render(prompt):
        return {"prompt": prompt}, b"\x89PNG..."

    m = memoize_calls(render, store=bucket, codec="paired", mode="record")
    assert m("cat") == ({"prompt": "cat"}, b"\x89PNG...")
    assert sorted(k.rsplit(".", 1)[1] for k in bucket) == ["bin", "json"]
    assert memoize_calls(render, store=bucket, codec="paired")("cat")[1] == b"\x89PNG..."
