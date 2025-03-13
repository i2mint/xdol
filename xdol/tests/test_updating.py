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
    assert stats["examined"] == 3
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 1
    assert stats["deleted"] == 0


def test_always_update_policy():
    """Test the ALWAYS_UPDATE policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 1, "c": 30}  # Note: 'a' has same value in both
    
    stats = update_with_policy(target, source, policy=DefaultPolicy.ALWAYS_UPDATE)
    
    assert target == {"a": 1, "b": 2, "c": 30}
    assert stats["updated"] == 1  # 'a' is updated even though value is the same
    assert stats["added"] == 1    # 'c' is added
    assert stats["unchanged"] == 1  # 'b' is unchanged


def test_prefer_target_policy():
    """Test the PREFER_TARGET policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}
    
    stats = update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)
    
    assert target == {"a": 1, "b": 2, "c": 30}
    assert stats["updated"] == 0  # 'a' is not updated
    assert stats["added"] == 1    # 'c' is added
    assert stats["unchanged"] == 2  # 'a' and 'b' are unchanged


def test_prefer_source_policy():
    """Test the PREFER_SOURCE policy."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}
    
    stats = update_with_policy(target, source, policy=DefaultPolicy.PREFER_SOURCE)
    
    assert target == {"a": 10, "b": 2, "c": 30}
    assert stats["updated"] == 1  # 'a' is updated
    assert stats["added"] == 1    # 'c' is added
    assert stats["unchanged"] == 1  # 'b' is unchanged


def test_custom_key_info_extractor():
    """Test using a custom key info extractor."""
    target = {"a": {"value": 1}, "b": {"value": 2}}
    source = {"a": {"value": 10}, "c": {"value": 30}}
    
    def get_value(key, obj):
        return obj.get("value")
    
    stats = update_with_policy(target, source, key_info=get_value)
    
    assert target["a"]["value"] == 10
    assert target["b"]["value"] == 2
    assert target["c"]["value"] == 30


def test_custom_decision_function():
    """Test using a custom decision function."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "b": 20, "d": 40}
    
    def update_only_for_a_and_d(key, target_info, source_info):
        if key in ["a", "d"] and source_info is not None:
            return KeyDecision.COPY
        return KeyDecision.SKIP
    
    stats = update_with_policy(target, source, policy=update_only_for_a_and_d)
    
    assert target == {"a": 10, "b": 2, "c": 3, "d": 40}
    assert stats["updated"] == 1  # 'a' is updated
    assert stats["added"] == 1    # 'd' is added
    assert stats["unchanged"] == 2  # 'b' and 'c' are unchanged


def test_keys_to_consider():
    """Test specifying a subset of keys to consider."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "b": 20, "d": 40}
    
    stats = update_with_policy(
        target, source, keys_to_consider={"a", "c", "d"}
    )
    
    assert target == {"a": 10, "b": 2, "c": 3, "d": 40}
    assert stats["examined"] == 3  # only 'a', 'c', 'd' examined
    assert stats["updated"] == 1   # 'a' updated
    assert stats["added"] == 1     # 'd' added
    assert stats["unchanged"] == 1  # 'c' unchanged


def test_delete_action():
    """Test a custom policy that can delete keys."""
    target = {"a": 1, "b": 2, "c": 3}
    source = {"a": 10, "d": 40}
    
    def delete_missing_from_source(key, target_info, source_info):
        if target_info is not None and source_info is None:
            return KeyDecision.DELETE
        if source_info is not None:
            return KeyDecision.COPY
        return KeyDecision.SKIP
    
    stats = update_with_policy(target, source, policy=delete_missing_from_source)
    
    assert target == {"a": 10, "d": 40}
    assert stats["updated"] == 1  # 'a' is updated
    assert stats["added"] == 1    # 'd' is added
    assert stats["deleted"] == 2  # 'b' and 'c' are deleted
    assert stats["unchanged"] == 0


def test_file_like_example():
    """Test with dictionary values that mimic files."""
    target = {
        "file1.txt": "original content",
        "file2.txt": "content to keep",
        "file3.txt": "old content"
    }
    
    source = {
        "file1.txt": "updated content",
        "file3.txt": "old content",  # Same as in target
        "file4.txt": "new file content"
    }
    
    stats = update_with_policy(target, source)
    
    assert target["file1.txt"] == "updated content"
    assert target["file2.txt"] == "content to keep"
    assert target["file3.txt"] == "old content"
    assert target["file4.txt"] == "new file content"
    
    assert stats["updated"] == 1  # file1.txt updated
    assert stats["added"] == 1    # file4.txt added
    assert stats["unchanged"] == 2  # file2.txt and file3.txt unchanged


def test_nested_metadata_example():
    """Test with nested dictionaries containing metadata."""
    # Create target and source with metadata
    yesterday = datetime(2023, 1, 1)
    today = datetime(2023, 1, 2)
    tomorrow = datetime(2023, 1, 3)
    
    target = {
        "file1.txt": {"modified_date": yesterday, "content": "original content"},
        "file2.txt": {"modified_date": today, "content": "keep this content"},
        "file3.txt": {"modified_date": today, "content": "old content"}
    }
    
    source = {
        "file1.txt": {"modified_date": today, "content": "updated content"},
        "file2.txt": {"modified_date": yesterday, "content": "outdated content"},
        "file3.txt": {"modified_date": today, "content": "old content"},
        "file4.txt": {"modified_date": tomorrow, "content": "new content"}
    }
    
    # Extract modified_date for comparison
    def get_modified_date(key, value):
        return value.get("modified_date")
    
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
        target, source, policy=source_is_newer, key_info=get_modified_date
    )
    
    assert target["file1.txt"]["content"] == "updated content"
    assert target["file2.txt"]["content"] == "keep this content"  # Not updated (source is older)
    assert target["file3.txt"]["content"] == "old content"  # Not updated (same date)
    assert target["file4.txt"]["content"] == "new content"  # Added
    
    assert stats["updated"] == 1  # file1.txt updated
    assert stats["added"] == 1    # file4.txt added
    assert stats["unchanged"] == 2  # file2.txt and file3.txt unchanged


# Test the convenience wrapper functions
def test_update_if_different():
    """Test the update_if_different function."""
    target = {"a": 1, "b": 2}
    source = {"a": 1, "c": 3}  # 'a' has same value
    
    stats = update_if_different(target, source)
    
    assert target == {"a": 1, "b": 2, "c": 3}
    assert stats["updated"] == 0  # 'a' not updated (same value)
    assert stats["added"] == 1    # 'c' added
    assert stats["unchanged"] == 2  # 'a' and 'b' unchanged


def test_update_all():
    """Test the update_all function."""
    target = {"a": 1, "b": 2}
    source = {"a": 1, "c": 3}
    
    stats = update_all(target, source)
    
    assert target == {"a": 1, "b": 2, "c": 3}
    assert stats["updated"] == 1  # 'a' updated even though value is the same
    assert stats["added"] == 1    # 'c' added


def test_update_missing_only():
    """Test the update_missing_only function."""
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 3}
    
    stats = update_missing_only(target, source)
    
    assert target == {"a": 1, "b": 2, "c": 3}
    assert stats["updated"] == 0  # 'a' not updated
    assert stats["added"] == 1    # 'c' added


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
    
    assert target["file1.txt"] == "updated content"     # Updated (different hash)
    assert target["file2.txt"] == "keep this content"   # Not updated (same hash)
    assert target["file3.txt"] == "new content"         # Added
    
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 1


def test_doctest_examples():
    """Test the examples from the docstrings."""
    # Example from update_with_policy docstring
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}
    stats = update_with_policy(target, source)
    assert stats["examined"] == 3
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 1
    assert stats["deleted"] == 0
    assert target == {"a": 10, "b": 2, "c": 30}
    
    # Example of PREFER_TARGET policy from docstring
    target = {"a": 1, "b": 2}
    source = {"a": 10, "c": 30}
    stats = update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)
    assert stats["examined"] == 3
    assert stats["updated"] == 0
    assert stats["added"] == 1
    assert stats["unchanged"] == 2
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
        target, 
        source, 
        target_timestamp=target_ts,
        source_timestamp=source_ts
    )
    
    # Check stats
    assert stats["updated"] == 1
    assert stats["added"] == 1
    assert stats["unchanged"] == 1
    
    # Check the actual content updates
    assert target["file1.txt"]["content"] == "updated"    # Updated (source is newer)
    assert target["file2.txt"]["content"] == "newer"      # Not updated (target is newer)
    assert target["file3.txt"]["content"] == "newest"     # Added (new key)


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
        target, 
        source, 
        target_timestamp=target_ts,
        source_timestamp=source_ts
    )
    
    # file2.txt should be skipped due to missing timestamp in target
    assert target["file1.txt"]["content"] == "updated"  # Updated (source is newer)
    assert target["file2.txt"]["content"] == "no timestamp"  # Unchanged (missing timestamp)


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
        assert target_store["file1.txt"] == b"updated content"  # Updated (source is newer)
        assert target_store["file2.txt"] == b"content to keep"  # Unchanged (only in target)
        assert target_store["file3.txt"] == b"new file content" # Added (only in source)
        
        # Check stats
        assert stats["updated"] == 1  # file1.txt
        assert stats["added"] == 1    # file3.txt
        assert stats["unchanged"] == 1  # file2.txt
        
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
            target_store, 
            source_store,
            keys_to_consider={"file1.txt", "file3.txt"}
        )
        
        # Verify updates
        assert target_store["file1.txt"] == b"updated content"   # Updated
        assert target_store["file2.txt"] == b"content to keep"   # Unchanged (not considered)
        assert target_store["file3.txt"] == b"new file content"  # Added
        
        # Check stats
        assert stats["examined"] == 2  # Only file1.txt and file3.txt
        assert stats["updated"] == 1   # file1.txt
        assert stats["added"] == 1     # file3.txt
        assert stats["unchanged"] == 0  # None
        
    finally:
        # Clean up
        shutil.rmtree(target_dir)
        shutil.rmtree(source_dir)