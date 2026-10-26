"""
Tests for the updated update_newer function and related utilities.
"""

import pytest
import os
import tempfile
import shutil
import time
from functools import partial
from typing import Dict, Any
from datetime import datetime
import hashlib

from xdol.updating import (
    update_with_policy,
    DefaultPolicy,
    KeyDecision,
    update_if_different,
    update_all,
    update_missing_only,
    update_newer,
    update_by_content_hash,
    update_files_by_timestamp,
    local_file_timestamp,
    VALUE_NOT_RETRIEVED,
)


from dol.filesys import Files

# """
# Tests for the update_policy module using pytest.

# This module contains pytest-based tests for all the functionality in update_policy.py,
# covering the various update policies and scenarios.
# """
# import pytest
# import hashlib
# from datetime import datetime
# from typing import Dict, Any

# from update_policy import (
#     update_with_policy,
#     DefaultPolicy,
#     KeyDecision,
#     update_if_different,
#     update_all,
#     update_missing_only,
#     update_newer,
#     update_by_content_hash
# )


def test_default_policy():
    """Test the default UPDATE_IF_DIFFERENT policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}

    stats = update_with_policy(target, source)

    assert target == {"a": 10, "b": 2, "c": 30}
    assert stats["examined"] == 2  # Changed from 3 to 2 (only source keys)
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 0  # Changed from 1 to 0 ('b' is not examined)
    assert stats["deleted"] == 0


def test_always_update_policy():
    """Test the ALWAYS_UPDATE policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 1, "c": 30}  # Note: 'a' has same value in both

    stats = update_with_policy(target, source, policy=DefaultPolicy.ALWAYS_UPDATE)

    assert target == {"a": 1, "b": 2, "c": 30}
    assert stats["updated"] == 1  # 'a' is updated even though value is the same
    assert stats["added"] == 1  # 'c' is added
    assert stats["unchanged"] == 0  # Changed from 1 to 0 ('b' is not examined)
    assert stats["examined"] == 2  # Now only examines 'a' and 'c'


def test_prefer_target_policy():
    """Test the PREFER_TARGET policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}

    stats = update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)

    assert target == {"a": 1, "b": 2, "c": 30}
    assert stats["updated"] == 0  # 'a' is not updated
    assert stats["added"] == 1  # 'c' is added
    assert stats["unchanged"] == 1  # Changed from 2 to 1 (only 'a' not 'b')
    assert stats["examined"] == 2  # Now only examines 'a' and 'c'


def test_prefer_source_policy():
    """Test the PREFER_SOURCE policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}

    stats = update_with_policy(target, source, policy=DefaultPolicy.PREFER_SOURCE)

    assert target == {"a": 10, "b": 2, "c": 30}
    assert stats["updated"] == 1  # 'a' is updated
    assert stats["added"] == 1  # 'c' is added
    assert stats["unchanged"] == 0  # Changed from 1 to 0 ('b' is not examined)
    assert stats["examined"] == 2  # Now only examines 'a' and 'c'


def test_custom_key_info_extractor():
    """Test using a custom key info extractor."""
    target = {"a": {"value": 1}, "b": {"value": 2}}
    source = {"a": {"value": 10}, "c": {"value": 30}}

    def get_target_value(key):
        return target.get(key, {}).get("value")

    def get_source_value(key):
        return source.get(key, {}).get("value")

    stats = update_with_policy(
        target,
        source,
        target_key_info=get_target_value,
        source_key_info=get_source_value,
    )

    assert target["a"]["value"] == 10
    assert target["b"]["value"] == 2
    assert target["c"]["value"] == 30


def test_custom_decision_function():
    """Test using a custom decision function."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "b": 20, "d": 40}

    def update_only_for_a_and_d(key, target_info, source_info):
        if key in ["a", "d"] and key in source:
            return KeyDecision.COPY
        return KeyDecision.SKIP

    stats = update_with_policy(target, source, policy=update_only_for_a_and_d)

    assert target == {"a": 10, "b": 2, "c": 3, "d": 40}
    assert stats["updated"] == 1  # 'a' is updated
    assert stats["added"] == 1  # 'd' is added
    assert (
        stats["unchanged"] == 1
    )  # Changed from 2 to 1 (only 'b' in source is examined)
    assert stats["examined"] == 3  # Now only examines 'a', 'b', and 'd'


def test_keys_to_consider():
    """Test specifying a subset of keys to consider."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "b": 20, "d": 40}

    stats = update_with_policy(target, source, keys_to_consider={"a", "c", "d"})

    assert target == {"a": 10, "b": 2, "c": 3, "d": 40}
    assert stats["examined"] == 3  # only 'a', 'c', 'd' examined
    assert stats["updated"] == 1  # 'a' updated
    assert stats["added"] == 1  # 'd' added
    assert stats["unchanged"] == 1  # 'c' unchanged


def test_source_and_target_keys():
    """Test explicitly using _source_and_target_keys."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "d": 40}

    # Import the source_and_target_keys function
    from xdol.updating import _source_and_target_keys

    stats = update_with_policy(target, source, keys_to_consider=_source_and_target_keys)

    assert target == {"a": 10, "b": 2, "c": 3, "d": 40}
    assert stats["examined"] == 4  # 'a', 'b', 'c', 'd' are all examined
    assert stats["updated"] == 1  # 'a' updated
    assert stats["added"] == 1  # 'd' added
    assert stats["unchanged"] == 2  # 'b' and 'c' unchanged


def test_delete_action():
    """Test a custom policy that can delete keys."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "d": 40}

    def delete_missing_from_source(key, target_info, source_info):
        if key in target and key not in source:
            return KeyDecision.DELETE
        if key in source:
            return KeyDecision.COPY
        return KeyDecision.SKIP

    # Need to use _source_and_target_keys to also consider keys in target
    from xdol.updating import _source_and_target_keys

    stats = update_with_policy(
        target,
        source,
        policy=delete_missing_from_source,
        keys_to_consider=_source_and_target_keys,
    )

    assert target == {"a": 10, "d": 40}
    assert stats["updated"] == 1  # 'a' is updated
    assert stats["added"] == 1  # 'd' is added
    assert stats["deleted"] == 2  # 'b' and 'c' are deleted
    assert stats["unchanged"] == 0


def test_file_like_example():
    """Test with dictionary values that mimic files."""
    target = {
        "file1.txt": "original content",
        "file2.txt": "content to keep",
        "file3.txt": "old content",
    }

    source = {
        "file1.txt": "updated content",
        "file3.txt": "old content",  # Same as in target
        "file4.txt": "new file content",
    }

    stats = update_with_policy(target, source)

    assert target["file1.txt"] == "updated content"
    assert target["file2.txt"] == "content to keep"
    assert target["file3.txt"] == "old content"
    assert target["file4.txt"] == "new file content"

    assert stats["updated"] == 1  # file1.txt updated
    assert stats["added"] == 1  # file4.txt added
    assert (
        stats["unchanged"] == 1
    )  # Changed from 2 to 1 (file3.txt only, file2.txt not examined)
    assert stats["examined"] == 3  # Only source keys examined


def test_nested_metadata_example():
    """Test with nested dictionaries containing metadata."""
    # Create target and source with metadata
    yesterday = datetime(2023, 1, 1)
    today = datetime(2023, 1, 2)
    tomorrow = datetime(2023, 1, 3)

    target = {
        "file1.txt": {"modified_date": yesterday, "content": "original content"},
        "file2.txt": {"modified_date": today, "content": "keep this content"},
        "file3.txt": {"modified_date": today, "content": "old content"},
    }

    source = {
        "file1.txt": {"modified_date": today, "content": "updated content"},
        "file2.txt": {"modified_date": yesterday, "content": "outdated content"},
        "file3.txt": {"modified_date": today, "content": "old content"},
        "file4.txt": {"modified_date": tomorrow, "content": "new content"},
    }

    # Extract modified_date for comparison
    def get_target_modified_date(key):
        return target.get(key, {}).get("modified_date")

    def get_source_modified_date(key):
        return source.get(key, {}).get("modified_date")

    # Update only if source is newer
    def source_is_newer(key, target_date, source_date):
        if target_date is None:
            return KeyDecision.COPY
        if source_date is None:
            return KeyDecision.SKIP
        if source_date > target_date:
            return KeyDecision.COPY
        return KeyDecision.SKIP

    stats = update_with_policy(
        target,
        source,
        policy=source_is_newer,
        target_key_info=get_target_modified_date,
        source_key_info=get_source_modified_date,
    )

    assert target["file1.txt"]["content"] == "updated content"
    assert (
        target["file2.txt"]["content"] == "keep this content"
    )  # Not updated (source is older)
    assert target["file3.txt"]["content"] == "old content"  # Not updated (same date)
    assert target["file4.txt"]["content"] == "new content"  # Added

    assert stats["updated"] == 1  # file1.txt updated
    assert stats["added"] == 1  # file4.txt added
    assert stats["unchanged"] == 2  # file2.txt and file3.txt unchanged


# Test the convenience wrapper functions
def test_update_if_different():
    """Test the update_if_different function."""
    target = {"a": 1, "b": 2}
    source = {"a": 1, "c": 3}  # 'a' has same value

    # Use closures instead of functions that require mapping argument
    def get_target_value(key):
        return target.get(key)

    def get_source_value(key):
        return source.get(key)

    stats = update_if_different(
        target,
        source,
        target_key_info=get_target_value,
        source_key_info=get_source_value,
    )

    assert target == {"a": 1, "b": 2, "c": 3}
    assert stats["updated"] == 0  # 'a' not updated (same value)
    assert stats["added"] == 1  # 'c' added
    assert stats["unchanged"] == 1  # Changed from 2 to 1 (only 'a' not 'b')
    assert stats["examined"] == 2  # Only source keys examined


def test_update_all():
    """Test the update_all function."""
    target = {"a": 1, "b": 2}
    source = {"a": 1, "c": 3}

    stats = update_all(target, source)

    assert target == {"a": 1, "b": 2, "c": 3}
    assert stats["updated"] == 1  # 'a' updated even though value is the same
    assert stats["added"] == 1  # 'c' added


def test_update_missing_only():
    """Test the update_missing_only function."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 3}

    stats = update_missing_only(target, source)

    assert target == {"a": 1, "b": 2, "c": 3}
    assert stats["updated"] == 0  # 'a' not updated
    assert stats["added"] == 1  # 'c' added


def test_update_by_content_hash():
    """Test the update_by_content_hash function."""
    target = {
        "file1.txt": "original content",
        "file2.txt": "keep this content",
    }

    source = {
        "file1.txt": "updated content",
        "file2.txt": "keep this content",  # Same content
        "file3.txt": "new content",
    }

    def hash_content(content):
        """Create a hash of content."""
        return hashlib.md5(content.encode()).hexdigest()

    stats = update_by_content_hash(target, source, hash_function=hash_content)

    assert target["file1.txt"] == "updated content"  # Updated (different hash)
    assert target["file2.txt"] == "keep this content"  # Not updated (same hash)
    assert target["file3.txt"] == "new content"  # Added

    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 1


def test_doctest_examples():
    """Test the examples from the docstrings."""
    # Example from update_with_policy docstring
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}
    stats = update_with_policy(target, source)
    assert stats["examined"] == 2  # Changed from 3 to 2 (only source keys)
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 0  # Changed from 1 to 0 ('b' not examined)
    assert stats["deleted"] == 0
    assert target == {"a": 10, "b": 2, "c": 30}

    # Example of PREFER_TARGET policy from docstring
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}
    stats = update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)
    assert stats["examined"] == 2  # Changed from 3 to 2 (only source keys)
    assert stats["updated"] == 0
    assert stats["added"] == 1
    assert stats["unchanged"] == 1  # Changed from 2 to 1 (only 'a' not 'b')
    assert stats["deleted"] == 0
    assert target == {"a": 1, "b": 2, "c": 30}


def test_update_newer_with_dict_timestamps():
    """Test update_newer with dictionary-based timestamps."""
    # Setup test data
    target = {
        "file1.txt": {"modified_date": "2022-01-01", "content": "old"},
        "file2.txt": {"modified_date": "2022-03-01", "content": "newer"},
    }

    source = {
        "file1.txt": {"modified_date": "2022-02-01", "content": "updated"},
        "file2.txt": {"modified_date": "2022-02-01", "content": "older"},
        "file3.txt": {"modified_date": "2022-04-01", "content": "newest"},
    }

    # Define timestamp extractors
    def get_timestamp(store, key):
        return store[key].get("modified_date")

    target_ts = lambda k: get_timestamp(target, k) if k in target else None
    source_ts = lambda k: get_timestamp(source, k) if k in source else None

    # Run the update
    stats = update_newer(
        target, source, target_timestamp=target_ts, source_timestamp=source_ts
    )

    # Check stats
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 1

    # Check the actual content updates
    assert target["file1.txt"]["content"] == "updated"  # Updated (source is newer)
    assert target["file2.txt"]["content"] == "newer"  # Not updated (target is newer)
    assert target["file3.txt"]["content"] == "newest"  # Added (new key)


def test_update_newer_with_missing_timestamps():
    """Test update_newer with missing timestamps."""
    # Setup test data with missing timestamp in one entry
    target = {
        "file1.txt": {"modified_date": "2022-01-01", "content": "old"},
        "file2.txt": {"content": "no timestamp"},  # Missing timestamp
    }

    source = {
        "file1.txt": {"modified_date": "2022-02-01", "content": "updated"},
        "file2.txt": {"modified_date": "2022-02-01", "content": "has timestamp"},
    }

    # Define timestamp extractors that might raise AttributeError
    def get_timestamp(store, key):
        try:
            return store[key].get("modified_date")
        except (KeyError, AttributeError):
            return None

    target_ts = lambda k: get_timestamp(target, k)
    source_ts = lambda k: get_timestamp(source, k)

    # Run the update
    stats = update_newer(
        target, source, target_timestamp=target_ts, source_timestamp=source_ts
    )

    # file2.txt should be skipped due to missing timestamp in target
    assert target["file1.txt"]["content"] == "updated"  # Updated (source is newer)
    assert (
        target["file2.txt"]["content"] == "no timestamp"
    )  # Unchanged (missing timestamp)


def test_local_file_timestamp_and_update_files_by_timestamp():
    """Test local_file_timestamp and update_files_by_timestamp using temporary directories."""
    # Create temporary directories
    target_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()

    try:
        # Create target files
        with open(os.path.join(target_dir, "file1.txt"), "w") as f:
            f.write("old content")

        with open(os.path.join(target_dir, "file2.txt"), "w") as f:
            f.write("content to keep")

        # Wait to ensure different timestamps
        time.sleep(0.1)

        # Create source files with newer timestamps
        with open(os.path.join(source_dir, "file1.txt"), "w") as f:
            f.write("updated content")

        # Wait again to ensure different timestamps
        time.sleep(0.1)

        with open(os.path.join(source_dir, "file3.txt"), "w") as f:
            f.write("new file content")

        # Create the Files stores for both directories
        target_store = Files(target_dir)
        source_store = Files(source_dir)

        # First, test the local_file_timestamp function
        target_file1_path = os.path.join(target_dir, "file1.txt")
        target_ts = local_file_timestamp(target_store, "file1.txt")
        direct_ts = os.stat(target_file1_path).st_mtime

        assert target_ts == direct_ts

        # Now test update_files_by_timestamp
        stats = update_files_by_timestamp(target_store, source_store)

        # Verify updates
        assert (
            target_store["file1.txt"] == b"updated content"
        )  # Updated (source is newer)
        assert (
            target_store["file2.txt"] == b"content to keep"
        )  # Unchanged (only in target)
        assert (
            target_store["file3.txt"] == b"new file content"
        )  # Added (only in source)

        # Check stats
        assert stats["updated"] == 1  # file1.txt
        assert stats["added"] == 1  # file3.txt
        assert stats["unchanged"] == 0  # Changed from 1 to 0 (file2.txt not examined)
        assert stats["examined"] == 2  # Only source keys examined

    finally:
        # Clean up
        shutil.rmtree(target_dir)
        shutil.rmtree(source_dir)


def test_update_files_by_timestamp_with_key_selection():
    """Test update_files_by_timestamp with specific keys to consider."""
    # Create temporary directories
    target_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()

    try:
        # Create target files
        with open(os.path.join(target_dir, "file1.txt"), "w") as f:
            f.write("old content")

        with open(os.path.join(target_dir, "file2.txt"), "w") as f:
            f.write("content to keep")

        # Wait to ensure different timestamps
        time.sleep(0.1)

        # Create source files with newer timestamps
        with open(os.path.join(source_dir, "file1.txt"), "w") as f:
            f.write("updated content")

        with open(os.path.join(source_dir, "file2.txt"), "w") as f:
            f.write("newer content")

        with open(os.path.join(source_dir, "file3.txt"), "w") as f:
            f.write("new file content")

        # Create the Files stores
        target_store = Files(target_dir)
        source_store = Files(source_dir)

        # Only consider file1.txt and file3.txt
        stats = update_files_by_timestamp(
            target_store, source_store, keys_to_consider={"file1.txt", "file3.txt"}
        )

        # Verify updates
        assert target_store["file1.txt"] == b"updated content"  # Updated
        assert (
            target_store["file2.txt"] == b"content to keep"
        )  # Unchanged (not considered)
        assert target_store["file3.txt"] == b"new file content"  # Added

        # Check stats
        assert stats["examined"] == 2  # Only file1.txt and file3.txt
        assert stats["updated"] == 1  # file1.txt
        assert stats["added"] == 1  # file3.txt
        assert stats["unchanged"] == 0  # None

    finally:
        # Clean up
        shutil.rmtree(target_dir)
        shutil.rmtree(source_dir)


def test_backfill_never_overwrites():
    from xdol.updating import backfill

    master, copy = {"a": 1}, {"a": 9, "b": 2}
    stats = backfill(copy=copy, master=master)
    assert master == {"a": 1, "b": 2} and stats.added == 1 and stats.updated == 0

    # A None value in master is still a value: it must not be overwritten
    master, copy = {"a": None}, {"a": 1}
    stats = backfill(copy=copy, master=master)
    assert master == {"a": None} and stats.updated == 0 and stats.added == 0


class _ReadCounter(dict):
    """A dict that counts value reads, through both ``[]`` and ``.get``."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reads = []

    def __getitem__(self, k):
        self.reads.append(k)
        return super().__getitem__(k)

    def get(self, k, default=None):
        self.reads.append(k)
        return super().get(k, default)


def test_backfill_reads_only_the_values_it_pushes():
    from xdol.updating import backfill

    master = _ReadCounter(a=1, shared=2)
    copy = _ReadCounter(shared=20, b=3, c=4)
    stats = backfill(copy=copy, master=master)
    assert master.reads == []
    assert sorted(copy.reads) == ["b", "c"]  # each pushed value read once
    assert dict(master) == {"a": 1, "shared": 2, "b": 3, "c": 4}
    assert (stats.added, stats.updated, stats.unchanged) == (2, 0, 1)


def test_reconcile_prefers_master_and_honours_policy():
    from xdol.updating import reconcile, DefaultPolicy

    # An identical key is left alone (the old PREFER_SOURCE default rewrote it)
    master, copy = {"a": 1, "b": 2, "c": 3}, {"a": 9, "c": 3}
    stats = reconcile(master=master, copy=copy)
    assert copy == {"a": 1, "b": 2, "c": 3}
    assert (stats.examined, stats.updated, stats.added, stats.unchanged) == (
        3,
        1,
        1,
        1,
    )
    master, copy = {"a": 1, "b": 2}, {"a": 9}
    stats = reconcile(master=master, copy=copy, policy=DefaultPolicy.PREFER_TARGET)
    assert copy == {"a": 9, "b": 2}
    assert (stats.updated, stats.added, stats.unchanged) == (0, 1, 1)


def test_verify_reports_three_sets_and_writes_nothing():
    from xdol.updating import verify

    copy, master = {"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 20, "d": 4}
    before = dict(copy), dict(master)
    r = verify(copy=copy, master=master)
    assert (r.only_in_copy, r.only_in_master, r.mismatched) == (["c"], ["d"], ["b"])
    assert not r.ok and (copy, master) == before
    assert verify(copy=copy, master=master, keys_to_consider=["a"]).ok


def test_verify_key_info_does_not_read_values():
    from xdol.updating import verify

    class NoRead(dict):
        def __getitem__(self, k):
            raise AssertionError("value read")

        def get(self, k, default=None):
            raise AssertionError("value read")

    # Values differ but key_info matches: ok, so the values were not compared
    c, m = NoRead(a=1, b=2), NoRead(a=10, b=20)
    sizes = {"a": 1, "b": 2}
    assert verify(copy=c, master=m, key_info_for=lambda s: sizes.__getitem__).ok

    # Values match but key_info differs: mismatch, so key_info is what decides
    c, m = NoRead(a=1, b=2), NoRead(a=1, b=2)
    etags = {id(c): {"a": "x", "b": "y"}, id(m): {"a": "x", "b": "z"}}
    r = verify(copy=c, master=m, key_info_for=lambda s: etags[id(s)].__getitem__)
    assert r.mismatched == ["b"] and not r.only_in_copy and not r.only_in_master


def test_content_hash_key_info_default():
    from xdol.updating import content_hash_key_info, verify

    def h(v):
        return content_hash_key_info({"k": v})("k")

    # bytes-like values are hashed by content
    assert h(b"abc") == h(bytearray(b"abc")) == h(memoryview(b"abc"))
    assert h(b"abc") != h(b"abd")
    assert isinstance(h(b"abc"), str)
    # str and bytes with the same content do not collide
    assert h("a") != h(b"a")
    assert h("a") == h("a")
    # lone surrogates hash instead of raising
    assert h("\ud800") != h("\ud801")
    # anything else is compared with ==
    assert verify(copy={"k": {"x": 1, "y": 2}}, master={"k": {"y": 2, "x": 1}}).ok
    assert verify(copy={"k": 1}, master={"k": 1.0}).ok
    assert verify(copy={"k": [1, 2]}, master={"k": [1, 3]}).mismatched == ["k"]


def test_master_copy_roles_are_keyword_only():
    # #22: swapping positional arguments to reconcile silently overwrote master
    from xdol.updating import backfill, reconcile, verify

    master, copy = {"a": 1}, {"a": 9}
    for f in (backfill, reconcile, verify):
        with pytest.raises(TypeError):
            f(master, copy)
    assert master == {"a": 1} and copy == {"a": 9}


def test_reconcile_key_info_for_skips_identical_without_reading_values():
    from xdol.updating import reconcile

    class NoRead(dict):
        def __getitem__(self, k):
            raise AssertionError(f"value read: {k}")

        def get(self, k, default=None):
            raise AssertionError(f"value read: {k}")

    etags = {"a": "x", "b": "y"}
    master, copy = NoRead(a=1, b=2), NoRead(a=1, b=2)
    s = reconcile(master=master, copy=copy, key_info_for=lambda s: etags.get)
    assert (s.examined, s.updated, s.unchanged) == (2, 0, 2)


def test_reconcile_does_not_rewrite_identical_values():
    from xdol.updating import reconcile

    class Writes(dict):
        written = []

        def __setitem__(self, k, v):
            self.written.append(k)
            super().__setitem__(k, v)

    copy = Writes(a=1, b=2)
    s = reconcile(master={"a": 1, "b": 3}, copy=copy)
    assert copy.written == ["b"] and (s.updated, s.unchanged) == (1, 1)


def test_keys_to_consider_callable_gets_master_then_copy():
    from xdol.updating import backfill, reconcile, verify

    seen = []

    def keys(master, copy):
        seen.append((master, copy))
        return ["a"]

    master, copy = {"a": 1, "b": 2}, {"a": 1}
    assert verify(copy=copy, master=master, keys_to_consider=keys).ok
    assert reconcile(master=master, copy=copy, keys_to_consider=keys).examined == 1
    assert backfill(master=master, copy=copy, keys_to_consider=keys).examined == 1
    assert all(m is master and c is copy for m, c in seen) and len(seen) == 3


def test_update_if_different_key_info_is_deprecated():
    from xdol.updating import update_if_different

    with pytest.warns(DeprecationWarning, match="target_key_info"):
        update_if_different({"a": 1}, {"a": 2}, key_info=lambda k, v: v)


def test_verify_keys_are_master_first_and_deduplicated():
    from xdol.updating import verify

    r = verify(master={"b": 1, "a": 1}, copy={"a": 2, "b": 2})
    assert r.mismatched == ["b", "a"]
    r = verify(master={"a": 1}, copy={"a": 2}, keys_to_consider=["a", "a"])
    assert r.mismatched == ["a"]


class _Incomparable:
    """``!=`` raises, or gives a non-bool whose truth can't be taken (like arrays)."""

    def __init__(self, ne):
        self.ne = ne

    def __ne__(self, other):
        return self.ne()


class _NoTruth:
    def __bool__(self):
        raise ValueError("truth value is ambiguous")


def _raise():
    raise TypeError("can't compare")


@pytest.mark.parametrize("ne", [_raise, _NoTruth])
def test_incomparable_values_count_as_different(ne):
    from xdol.updating import reconcile, verify

    master, copy = {"a": _Incomparable(ne)}, {"a": _Incomparable(ne)}
    assert verify(master=master, copy=copy).mismatched == ["a"]
    assert reconcile(master=master, copy=copy).updated == 1
    assert copy["a"] is master["a"]


def test_numpy_values_are_compared_safely():
    np = pytest.importorskip("numpy")
    from xdol.updating import reconcile, verify

    master, copy = {"a": np.array([1, 2])}, {"a": np.array([1, 3])}
    assert verify(master=master, copy=copy).mismatched == ["a"]
    assert reconcile(master=master, copy=copy).updated == 1
    assert copy["a"] is master["a"]
    master, copy = {"a": np.array([1, 2])}, {"a": np.array([1, 2])}  # equal
    assert verify(master=master, copy=copy).ok
    assert reconcile(master=master, copy=copy).unchanged == 1


def test_copy_writes_what_getitem_returns():
    # the value compared is the value written: a store decoding in __getitem__ only
    # (dict.get bypasses it) must not have its raw value copied
    from xdol.updating import reconcile, update_with_policy

    class Decoding(dict):
        def __getitem__(self, k):
            return super().__getitem__(k).decode()

    master, copy = Decoding(a=b"new", b=b"same"), {"a": "old", "b": "same"}
    s = reconcile(master=master, copy=copy)
    assert copy == {"a": "new", "b": "same"} and (s.updated, s.unchanged) == (1, 1)
    target = {}
    update_with_policy.all(target, Decoding(a=b"x"))
    assert target == {"a": "x"}


def test_reconcile_reads_each_master_value_at_most_once():
    from xdol.updating import reconcile, DefaultPolicy

    def pair():
        return _ReadCounter(same=1, changed=2, new=3), _ReadCounter(same=1, changed=9)

    # Default: each value read once, to compare; a changed one is not read again
    master, copy = pair()
    s = reconcile(master=master, copy=copy)
    assert sorted(master.reads) == ["changed", "new", "same"]
    assert (s.updated, s.added, s.unchanged) == (1, 1, 1)

    # Presence-only policies read only the values they copy, once each
    master, copy = pair()
    reconcile(master=master, copy=copy, policy=DefaultPolicy.PREFER_SOURCE)
    assert sorted(master.reads) == ["changed", "new", "same"] and copy.reads == []
    master, copy = pair()
    reconcile(master=master, copy=copy, policy=DefaultPolicy.PREFER_TARGET)
    assert master.reads == ["new"] and copy.reads == []
