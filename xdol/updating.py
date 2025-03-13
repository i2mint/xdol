"""
Function to replace the update_newer function in update_policy.py.
"""

"""
Functions for controlled mapping updates with customizable policies.

This module provides flexible functions to update a target mapping from a source
mapping using customizable policies to determine which items to update.
"""
from typing import (
    Mapping,
    MutableMapping,
    Any,
    Callable,
    Optional,
    Dict,
    Union,
    TypeVar,
    Iterator,
    Tuple,
    Set,
    Protocol,
)
import os
from collections.abc import Iterable
from enum import Enum, auto
from functools import partial
from typing_extensions import Protocol
from dataclasses import dataclass

from dol.dig import inner_most_key


K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


def add_as_attribute_of(obj, name=None):
    """
    Decorator to add the function as an attribute of the object.

    Args:
        obj: The object to which the function will be added
        name: The name of the attribute to be added, if None, uses the function name

    Returns:
        The decorator function

    Examples:

    >>> def foo():
    ...     return "bar"
    ...
    >>> @add_as_attribute_of(foo)
    ... def baz():
    ...     return "qux"
    ...
    >>> foo.baz()
    'qux'
    """

    def decorator(func):
        setattr(obj, name or func.__name__, func)
        return func

    return decorator


class DefaultPolicy(Enum):
    """Standard policies for updating mappings."""

    ALWAYS_UPDATE = auto()  # Always update target with source values
    UPDATE_IF_DIFFERENT = auto()  # Update only if values differ
    PREFER_TARGET = auto()  # Keep target values if they exist
    PREFER_SOURCE = auto()  # Always use source values when available


class KeyDecision(Enum):
    """Decisions for individual keys during update."""

    COPY = auto()  # Copy from source to target
    SKIP = auto()  # Don't copy, keep target as is
    DELETE = auto()  # Delete from target


class KeyInfoExtractor(Protocol):
    """Protocol for functions that extract comparison information from values."""

    def __call__(self, key: K, value: V) -> Any:
        """Extract comparison information from a value."""
        ...


class UpdateDecider(Protocol):
    """Protocol for functions that decide whether to update a key."""

    def __call__(
        self, key: K, target_info: Optional[Any], source_info: Optional[Any]
    ) -> KeyDecision:
        """
        Decide whether to update a key based on comparison info.

        Args:
            key: The key being considered
            target_info: Comparison info for target value, None if key not in target
            source_info: Comparison info for source value, None if key not in source

        Returns:
            KeyDecision indicating what to do with this key
        """
        ...


@dataclass
class UpdateStats:
    """Statistics for an update operation."""

    examined: int = 0
    updated: int = 0
    added: int = 0
    unchanged: int = 0
    deleted: int = 0

    def as_dict(self) -> Dict[str, int]:
        """Convert stats to a dictionary."""
        return {
            "examined": self.examined,
            "updated": self.updated,
            "added": self.added,
            "unchanged": self.unchanged,
            "deleted": self.deleted,
        }


def _key_info_identity(key: K, value: V) -> V:
    """Default key info extractor that returns the value itself."""
    return value


def _update_if_different_decider(
    key: K, target_info: Any, source_info: Any
) -> KeyDecision:
    """Default decision function that updates if values differ."""
    if target_info is None and source_info is not None:
        return KeyDecision.COPY
    if target_info is not None and source_info is None:
        return KeyDecision.SKIP
    if target_info != source_info:
        return KeyDecision.COPY
    return KeyDecision.SKIP


def _always_update_decider(key: K, target_info: Any, source_info: Any) -> KeyDecision:
    """Decision function that always updates from source."""
    if source_info is None:
        return KeyDecision.SKIP
    return KeyDecision.COPY


def _prefer_target_decider(key: K, target_info: Any, source_info: Any) -> KeyDecision:
    """Decision function that keeps target values if they exist."""
    if target_info is None and source_info is not None:
        return KeyDecision.COPY
    return KeyDecision.SKIP


def _prefer_source_decider(key: K, target_info: Any, source_info: Any) -> KeyDecision:
    """Decision function that always uses source values when available."""
    if source_info is None:
        return KeyDecision.SKIP
    return KeyDecision.COPY


def _get_standard_decider(policy: DefaultPolicy) -> UpdateDecider:
    """Get a standard decision function for a given policy."""
    if policy == DefaultPolicy.ALWAYS_UPDATE:
        return _always_update_decider
    elif policy == DefaultPolicy.UPDATE_IF_DIFFERENT:
        return _update_if_different_decider
    elif policy == DefaultPolicy.PREFER_TARGET:
        return _prefer_target_decider
    elif policy == DefaultPolicy.PREFER_SOURCE:
        return _prefer_source_decider
    else:
        raise ValueError(f"Unknown policy: {policy}")


def _union_keys(mappings: Iterable[Mapping]) -> Set[K]:
    """Get the union of keys from multiple mappings."""
    keys = set()
    for mapping in mappings:
        keys.update(mapping.keys())
    return keys


def _get_key_decisions(
    keys: Set[K],
    target: Mapping,
    source: Mapping,
    decider: UpdateDecider,
    key_info: KeyInfoExtractor,
) -> Iterator[Tuple[K, KeyDecision]]:
    """
    Get decisions for each key regarding update action.

    Args:
        keys: Set of keys to consider
        target: Target mapping
        source: Source mapping
        decider: Function to decide what to do with each key
        key_info: Function to extract comparison info from values

    Returns:
        Iterator of (key, decision) pairs
    """
    for key in keys:
        target_value = target.get(key, None)
        source_value = source.get(key, None)

        target_info = None if target_value is None else key_info(key, target_value)
        source_info = None if source_value is None else key_info(key, source_value)

        decision = decider(key, target_info, source_info)
        yield key, decision


def update_with_policy(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    policy: Union[DefaultPolicy, UpdateDecider] = DefaultPolicy.UPDATE_IF_DIFFERENT,
    key_info: Optional[KeyInfoExtractor] = None,
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update a target mapping with values from a source mapping using a customizable policy.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        policy: Either a DefaultPolicy enum value or a custom decision function
        key_info: Function to extract comparison info from values for decision-making
        keys_to_consider: Specific set of keys to consider, if None, uses union of all keys

    Returns:
        Dictionary with statistics about the update operation

    Examples:
        >>> target = {"a": 1, "b": 2}
        >>> source = {"a": 10, "c": 30}
        >>> update_with_policy(target, source)
        {'examined': 3, 'updated': 1, 'added': 1, 'unchanged': 1, 'deleted': 0}
        >>> target
        {'a': 10, 'b': 2, 'c': 30}

        >>> # Using PREFER_TARGET policy
        >>> target = {"a": 1, "b": 2}
        >>> source = {"a": 10, "c": 30}
        >>> update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)
        {'examined': 3, 'updated': 0, 'added': 1, 'unchanged': 2, 'deleted': 0}
        >>> target
        {'a': 1, 'b': 2, 'c': 30}
    """
    key_info_func = key_info or _key_info_identity

    # Determine the decision function
    if isinstance(policy, DefaultPolicy):
        decider = _get_standard_decider(policy)
    else:
        decider = policy

    # Determine keys to consider
    if keys_to_consider is None:
        keys_to_consider = _union_keys([target, source])

    stats = UpdateStats()

    # Process each key according to the decided action
    for key, decision in _get_key_decisions(
        keys_to_consider, target, source, decider, key_info_func
    ):
        stats.examined += 1

        if decision == KeyDecision.COPY:
            if key in target:
                target[key] = source[key]
                stats.updated += 1
            else:
                target[key] = source[key]
                stats.added += 1
        elif decision == KeyDecision.DELETE:
            if key in target:
                del target[key]
                stats.deleted += 1
        else:  # SKIP
            stats.unchanged += 1

    return stats.as_dict()


# Common update policies as convenience functions


@add_as_attribute_of(update_with_policy, name="if_different")
def update_if_different(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    key_info: Optional[KeyInfoExtractor] = None,
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update target with source values only if they differ.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        key_info: Function to extract comparison info from values
        keys_to_consider: Specific set of keys to consider

    Returns:
        Dictionary with statistics about the update operation
    """
    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.UPDATE_IF_DIFFERENT,
        key_info=key_info,
        keys_to_consider=keys_to_consider,
    )


@add_as_attribute_of(update_with_policy, name="all")
def update_all(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update target with all source values, equivalent to dict.update().

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        keys_to_consider: Specific set of keys to consider

    Returns:
        Dictionary with statistics about the update operation
    """
    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.ALWAYS_UPDATE,
        keys_to_consider=keys_to_consider,
    )


@add_as_attribute_of(update_with_policy, name="missing_only")
def update_missing_only(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update target with source values only for keys not in target.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        keys_to_consider: Specific set of keys to consider

    Returns:
        Dictionary with statistics about the update operation
    """
    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.PREFER_TARGET,
        keys_to_consider=keys_to_consider,
    )


@add_as_attribute_of(update_with_policy, name="by_content_hash")
def update_by_content_hash(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    hash_function: Callable[[V], Any],
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update target with source values only if their hash differs.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        hash_function: Function to generate a hash of a value
        keys_to_consider: Specific set of keys to consider

    Returns:
        Dictionary with statistics about the update operation
    """

    def _get_hash(key: K, value: V) -> Any:
        return hash_function(value)

    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.UPDATE_IF_DIFFERENT,
        key_info=_get_hash,
        keys_to_consider=keys_to_consider,
    )


# from typing import Mapping, MutableMapping, Any, Callable, Dict, TypeVar, Set, Optional
# import os
# from datetime import datetime
# from functools import partial
# from update_policy import update_with_policy, KeyDecision
#

# K = TypeVar('K')
# V = TypeVar('V')


def local_file_timestamp(store, key) -> float:
    """
    Get the modified timestamp of a file in a local file store.

    Uses inner_most_key to handle relative paths correctly, resolving to
    the full path before getting the timestamp.

    Args:
        store: A mapping whose keys resolve to file paths
        key: A key in the store

    Returns:
        float: The modification timestamp of the file
    """
    # Get the full path using inner_most_key
    full_path = inner_most_key(store, key)
    # Return the modification timestamp
    return os.stat(full_path).st_mtime


@add_as_attribute_of(update_with_policy, name="newer")
def update_newer(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    target_timestamp: Callable[[K], Any],
    source_timestamp: Callable[[K], Any],
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update target with source values only if source has a newer timestamp.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        target_timestamp: Function(key) -> timestamp that extracts timestamp from target for a key
        source_timestamp: Function(key) -> timestamp that extracts timestamp from source for a key
        keys_to_consider: Specific set of keys to consider

    Returns:
        Dictionary with statistics about the update operation

    Example:
        >>> import tempfile, os
        >>> from datetime import datetime
        >>> # Create a function to get timestamps from values that contain timestamps
        >>> def get_timestamp(store, key):
        ...     return store[key].get("modified_date")
        >>>
        >>> # Setup
        >>> target = {
        ...     "file1.txt": {"modified_date": "2022-01-01", "content": "old"},
        ...     "file2.txt": {"modified_date": "2022-03-01", "content": "newer"}
        ... }
        >>> source = {
        ...     "file1.txt": {"modified_date": "2022-02-01", "content": "updated"},
        ...     "file2.txt": {"modified_date": "2022-02-01", "content": "older"},
        ...     "file3.txt": {"modified_date": "2022-04-01", "content": "newest"}
        ... }
        >>>
        >>> # Create timestamp functions
        >>> target_ts = lambda k: get_timestamp(target, k)
        >>> source_ts = lambda k: get_timestamp(source, k)
        >>>
        >>> # Update
        >>> update_newer(target, source, target_timestamp=target_ts, source_timestamp=source_ts)
        {'examined': 3, 'updated': 1, 'added': 1, 'unchanged': 1, 'deleted': 0}
        >>>
        >>> # Verify results
        >>> target["file1.txt"]["content"]  # Updated (source is newer)
        'updated'
        >>> target["file2.txt"]["content"]  # Not updated (target is newer)
        'newer'
        >>> target["file3.txt"]["content"]  # Added
        'newest'
    """

    def _newer_decider(key: K, target_value: Any, source_value: Any) -> KeyDecision:
        """Decision function based on timestamp comparison."""
        if source_value is None:
            return KeyDecision.SKIP
        if target_value is None:
            return KeyDecision.COPY

        try:
            # Get timestamps using the provided functions
            source_ts = source_timestamp(key)
            target_ts = target_timestamp(key)

            # If either timestamp is None, skip update (can't compare)
            if source_ts is None or target_ts is None:
                return KeyDecision.SKIP

            # Update if source is newer
            if source_ts > target_ts:
                return KeyDecision.COPY
            return KeyDecision.SKIP

        except (KeyError, FileNotFoundError, AttributeError, TypeError):
            # If we can't get or compare timestamps, skip
            return KeyDecision.SKIP

    return update_with_policy(
        target, source, policy=_newer_decider, keys_to_consider=keys_to_consider
    )


# Convenience function for file-based stores
@add_as_attribute_of(update_newer, name="files_by_timestamp")
def update_files_by_timestamp(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    keys_to_consider: Optional[Set[K]] = None,
) -> Dict[str, int]:
    """
    Update a target file store with files from a source store based on modification times.

    This is a convenience wrapper around update_newer that uses local_file_timestamp
    to compare file modification times.

    Args:
        target: The target file store to be updated
        source: The source file store containing potential updates
        keys_to_consider: Specific set of keys to consider

    Returns:
        Dictionary with statistics about the update operation
    """
    target_ts = partial(local_file_timestamp, target)
    source_ts = partial(local_file_timestamp, source)

    return update_newer(
        target,
        source,
        target_timestamp=target_ts,
        source_timestamp=source_ts,
        keys_to_consider=keys_to_consider,
    )
