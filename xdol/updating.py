"""Controlled updates of one mapping from another, with customizable policies.

``update_with_policy`` decides, key by key, whether to copy a source item into a
target mapping. It goes beyond ``dict.update()`` with:

- built-in policies (always update, update if different, prefer target, prefer source, an alias of always update)
- custom decision functions and custom "key info" extractors (compare sizes,
  hashes or timestamps instead of reading values)
- statistics about what was examined, updated, added, unchanged or deleted
- specialized entry points (content-hash comparison, timestamps, local files)

A last section names the operations on a master / copy pair (companions of
``xdol.tiering``): ``backfill`` (push to master what only the copy has),
``reconcile`` (update the copy from master) and ``verify`` (compare without
writing).

Custom update logic with a hash function:

>>> target = {"a": "hello", "b": "world"}
>>> source = {"a": "hello!", "c": "python"}
>>> update_by_content_hash(target, source, hash_function=lambda x: len(x))
{'examined': 2, 'updated': 1, 'added': 1, 'unchanged': 0, 'deleted': 0}
>>> target
{'a': 'hello!', 'b': 'world', 'c': 'python'}

Using convenience methods:

>>> update_with_policy.missing_only(target, source)
{'examined': 2, 'updated': 0, 'added': 0, 'unchanged': 2, 'deleted': 0}
>>> target
{'a': 'hello!', 'b': 'world', 'c': 'python'}
"""

from typing import (
    Any,
    Optional,
    Dict,
    Union,
    TypeVar,
    Tuple,
    Set,
)
from collections.abc import (
    Mapping,
    MutableMapping,
    Iterable,
    Generator,
    Callable,
    Iterator,
)
import os
import warnings
from enum import Enum, auto
from functools import partial
from typing_extensions import Protocol
import hashlib
from dataclasses import dataclass, field

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


class StringEnum(str, Enum):
    """Enum where members are also (and must be) strings."""

    def __str__(self):
        return self.value


class DefaultPolicy(StringEnum):
    """Standard policies for updating mappings."""

    ALWAYS_UPDATE = "ALWAYS_UPDATE"  # Always update target with source values
    UPDATE_IF_DIFFERENT = "UPDATE_IF_DIFFERENT"  # Update only if values differ
    PREFER_TARGET = "PREFER_TARGET"  # Keep target values if they exist
    PREFER_SOURCE = "ALWAYS_UPDATE"  # Alias of ALWAYS_UPDATE (one decider, not two)

    @classmethod
    def _missing_(cls, value):
        # The string "PREFER_SOURCE" (saved configs, callers) still resolves
        if value == "PREFER_SOURCE":
            return cls.ALWAYS_UPDATE
        return None


class KeyDecision(StringEnum):
    """Decisions for individual keys during update."""

    COPY = "COPY"  # Copy from source to target
    SKIP = "SKIP"  # Don't copy, keep target as is
    DELETE = "DELETE"  # Delete from target


class KeyInfoExtractor(Protocol):
    """Protocol for functions that extract comparison information from values."""

    def __call__(self, key: K) -> Any:
        """Extract comparison information for a key."""
        ...


KeyInfoFor = Callable[[Mapping], KeyInfoExtractor]
"""``key_info_for(store)``: the ``key -> info`` extractor for ``store``."""


class UpdateDecider(Protocol):
    """Protocol for functions that decide whether to update a key."""

    def __call__(
        self,
        key: K,
        target_info: Any,
        source_info: Any,
    ) -> KeyDecision:
        """
        Decide whether to update a key based on comparison info.

        Args:
            key: The key being considered
            target_info: Comparison info for target, ``missing`` if key not in target
            source_info: Comparison info for source, ``missing`` if key not in source
            (``missing`` is ``None`` unless ``update_with_policy(missing=MISSING)``;
            see i2mint/xdol#21)

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

    def as_dict(self) -> dict[str, int]:
        """Convert stats to a dictionary."""
        return {
            "examined": self.examined,
            "updated": self.updated,
            "added": self.added,
            "unchanged": self.unchanged,
            "deleted": self.deleted,
        }


# Define a sentinel object to indicate values that haven't been retrieved yet
class NotRetrieved:
    """Sentinel object indicating a value that hasn't been retrieved yet."""

    def __repr__(self):
        return "<NotRetrieved>"


VALUE_NOT_RETRIEVED = NotRetrieved()


class _Missing:
    """Type of :data:`MISSING`."""

    def __repr__(self):
        return "MISSING"

    def __reduce__(self):  # stays the one MISSING through pickle and deepcopy
        return "MISSING"


MISSING = _Missing()
"""Info given to a decider for a key that is absent from a mapping. Unlike ``None``,
it cannot be mistaken for a key whose value or info is ``None``."""


def _key_info_from_mapping(mapping: Mapping[K, V], key: K) -> Any:
    """Default key info extractor: the value, read as ``mapping[key]`` (the read a copy
    would make, so the compared value is also the one written)."""
    return mapping[key]


def _get_key_decisions(
    keys: Iterable[K],
    target: Mapping,
    source: Mapping,
    decider: UpdateDecider,
    target_key_info: KeyInfoExtractor | None = None,
    source_key_info: KeyInfoExtractor | None = None,
    *,
    missing: Any = None,
) -> Iterator[tuple[K, KeyDecision, Any]]:
    """
    Get decisions for each key regarding update action.

    Key info is fetched only for a key that is in that mapping; ``missing`` stands in
    for the info of one that isn't.

    Args:
        keys: Keys to consider
        target: Target mapping
        source: Source mapping
        decider: Function to decide what to do with each key
        target_key_info: Function to extract comparison info from target (optional)
        source_key_info: Function to extract comparison info from source (optional)
        missing: What the decider gets as the info of an absent key

    Returns:
        Iterator of (key, decision, source_info) triples; ``source_info`` is
        :data:`MISSING` when the key is not in source.
    """
    # Use default extractors if none provided
    target_info_func = target_key_info or partial(_key_info_from_mapping, target)
    source_info_func = source_key_info or partial(_key_info_from_mapping, source)

    for key in keys:
        target_info = target_info_func(key) if key in target else missing
        source_in = key in source
        source_info = source_info_func(key) if source_in else missing

        decision = decider(key, target_info, source_info)
        yield key, decision, (source_info if source_in else MISSING)


def _differ(a, b) -> bool:
    """Whether ``a != b``; ``True`` ("different") when they cannot be compared.

    An elementwise result (an array) counts as different if any element differs.
    """
    try:
        result = a != b
        try:
            return bool(result)
        except (TypeError, ValueError):  # e.g. an array has no single truth value
            return bool(result.any())
    except Exception:
        return True


# The standard deciders always get MISSING for an absent key (update_with_policy
# passes missing=MISSING to them), so a None value or info is just a value.
def _update_if_different_decider(
    key: K,
    target_info: Any,
    source_info: Any,
) -> KeyDecision:
    """Default decision function that updates if values differ."""
    if source_info is MISSING:
        return KeyDecision.SKIP
    if target_info is MISSING or _differ(target_info, source_info):
        return KeyDecision.COPY
    return KeyDecision.SKIP


def _always_update_decider(
    key: K,
    target_info: Any,
    source_info: Any,
) -> KeyDecision:
    """Decision function that always updates from source (``PREFER_SOURCE`` too)."""
    return KeyDecision.SKIP if source_info is MISSING else KeyDecision.COPY


def _prefer_target_decider(
    key: K,
    target_info: Any,
    source_info: Any,
) -> KeyDecision:
    """Decision function that keeps target values if they exist."""
    if target_info is MISSING and source_info is not MISSING:
        return KeyDecision.COPY
    return KeyDecision.SKIP


_STANDARD_DECIDERS = {
    DefaultPolicy.ALWAYS_UPDATE: _always_update_decider,
    DefaultPolicy.UPDATE_IF_DIFFERENT: _update_if_different_decider,
    DefaultPolicy.PREFER_TARGET: _prefer_target_decider,
}


def _get_standard_decider(policy: DefaultPolicy) -> UpdateDecider:
    """Get a standard decision function for a given policy."""
    try:
        return _STANDARD_DECIDERS[DefaultPolicy(policy)]
    except (KeyError, ValueError):
        raise ValueError(f"Unknown policy: {policy}") from None


def union_iter(*iterables: Iterable[T]) -> Generator[T, None, None]:
    """
    Generator yielding unique hashable items from an iterable of iterables.

    Args:
        iterables: An iterable where each element is itself an iterable of hashable items.

    Yields:
        Unique items from the nested iterables, in order of first appearance.

    Example:
        >>> list(union_iter(['a', 'b'], ['b', 'c']))
        ['a', 'b', 'c']
        >>> list(union_iter({'a': 1, 'b': 2}.keys(), {'b': 3, 'c': 4}.values()))
        ['a', 'b', 3, 4]
    """
    seen = set()
    for iterable in iterables:
        for item in iterable:
            if item not in seen:
                seen.add(item)
                yield item


def print_all_but_skips(key: K, decision: KeyDecision, *, print_func=print):
    """Print all decisions except SKIP."""
    if decision != KeyDecision.SKIP:
        print_func(f"{decision}: {key}")


def _source_and_target_keys(source: Mapping, target: Mapping):
    """Return the union of keys from both source and target mappings."""
    return source.keys() | target.keys()


def _just_source_keys(source: Mapping, target: Mapping):
    """Return only the keys from the source mapping."""
    return source.keys()


def _present(key: K) -> bool:
    """Presence-only key info: never reads a value.

    ``update_with_policy`` only asks for the info of keys a mapping contains, so a
    constant non-``None`` answer means "present" (even when the value is ``None``).
    """
    return True


# The standard policies that look at presence only, never at the infos' content
_PRESENCE_ONLY_POLICIES = (
    DefaultPolicy.ALWAYS_UPDATE,
    DefaultPolicy.PREFER_TARGET,
)  # (PREFER_SOURCE is an alias of ALWAYS_UPDATE)


Target = MutableMapping
Source = Mapping
KeysToConsider = Union[Iterable[K], Callable[[Source, Target], Iterable[K]]]


def update_with_policy(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    policy: DefaultPolicy | str | UpdateDecider = DefaultPolicy.UPDATE_IF_DIFFERENT,
    target_key_info: KeyInfoExtractor | None = None,
    source_key_info: KeyInfoExtractor | None = None,
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
    missing: Any = None,
) -> dict[str, int]:
    """
    Update a target mapping with values from a source mapping using a customizable policy.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        policy: Either a DefaultPolicy enum value or a custom decision function
        target_key_info: Function to extract comparison info from target keys
        source_key_info: Function to extract comparison info from source keys
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, will print debug information, if callable, call with
            (key, decision) on each key that is looped over
        missing: What a *custom* decider gets as the info of a key absent from a
            mapping. ``None`` (the default, for now) can't be told from a present key
            whose info is ``None``; pass :data:`MISSING` to tell them apart (i2mint/xdol#21).
            The standard policies always use ``MISSING`` and, when they look at
            presence only, read no value or info to decide.

    Returns:
        Dictionary with statistics about the update operation

    Examples:
        >>> target = {"a": 1, "b": 2}
        >>> source = {"a": 10, "c": 30}
        >>> update_with_policy(target, source)
        {'examined': 2, 'updated': 1, 'added': 1, 'unchanged': 0, 'deleted': 0}
        >>> target
        {'a': 10, 'b': 2, 'c': 30}

        >>> # Using PREFER_TARGET policy
        >>> target = {"a": 1, "b": 2}
        >>> source = {"a": 10, "c": 30}
        >>> update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)
        {'examined': 2, 'updated': 0, 'added': 1, 'unchanged': 1, 'deleted': 0}
        >>> target
        {'a': 1, 'b': 2, 'c': 30}
    """
    if verbose is False:
        verbose = lambda k, d: None
    elif verbose is True:
        verbose = print_all_but_skips
    else:
        assert callable(verbose), "Verbose must be a callable or boolean"

    # Determine the decision function
    if isinstance(policy, (DefaultPolicy, str)):
        decider = _get_standard_decider(policy)
        missing = MISSING
        if DefaultPolicy(policy) in _PRESENCE_ONLY_POLICIES:
            target_key_info = target_key_info or _present
            source_key_info = source_key_info or _present
    elif callable(policy):
        decider = policy
    else:
        raise ValueError(f"Unknown policy: {policy}")

    # Determine keys to consider
    if callable(keys_to_consider):
        keys_to_consider_factory = keys_to_consider
        keys_to_consider = keys_to_consider_factory(source, target)

    stats = UpdateStats()

    # Process each key according to the decided action
    for key, decision, source_info in _get_key_decisions(
        keys_to_consider,
        target,
        source,
        decider,
        target_key_info,
        source_key_info,
        missing=missing,
    ):
        verbose(key, decision)

        stats.examined += 1

        if decision == KeyDecision.COPY:
            # The default info is the value itself: don't download it twice
            if source_key_info is None and source_info is not MISSING:
                value = source_info
            else:
                value = source[key]
            if key in target:
                target[key] = value
                stats.updated += 1
            else:
                target[key] = value
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
    key_info=None,  # For backward compatibility
    target_key_info=None,
    source_key_info=None,
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
) -> dict[str, int]:
    """
    Update target with source values only if they differ.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        key_info: (Deprecated) Function to extract comparison info from values
        target_key_info: Function to extract comparison info from target keys
        source_key_info: Function to extract comparison info from source keys
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, print debug information, if callable, call with
            (key, decision) on each key that is looped over

    Returns:
        Dictionary with statistics about the update operation
    """
    if key_info is not None:
        warnings.warn(
            "update_if_different(key_info=...) is deprecated: pass "
            "target_key_info/source_key_info, each a (key) -> info function",
            DeprecationWarning,
            stacklevel=2,
        )
        target_key_info = target_key_info or (lambda k: key_info(k, target.get(k)))
        source_key_info = source_key_info or (lambda k: key_info(k, source.get(k)))

    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.UPDATE_IF_DIFFERENT,
        target_key_info=target_key_info,
        source_key_info=source_key_info,
        keys_to_consider=keys_to_consider,
        verbose=verbose,
    )


@add_as_attribute_of(update_with_policy, name="all")
def update_all(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
) -> dict[str, int]:
    """
    Update target with all source values, equivalent to dict.update().

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, print debug information, if callable, call with
            (key, decision) on each key that is looped over

    Returns:
        Dictionary with statistics about the update operation
    """
    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.ALWAYS_UPDATE,
        keys_to_consider=keys_to_consider,
        verbose=verbose,
    )


@add_as_attribute_of(update_with_policy, name="missing_only")
def update_missing_only(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
) -> dict[str, int]:
    """
    Update target with source values only for keys not in target.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, print debug information, if callable, call with
            (key, decision) on each key that is looped over

    Returns:
        Dictionary with statistics about the update operation
    """
    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.PREFER_TARGET,
        keys_to_consider=keys_to_consider,
        verbose=verbose,
    )


@add_as_attribute_of(update_with_policy, name="by_content_hash")
def update_by_content_hash(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    hash_function: Callable[[V], Any],
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
) -> dict[str, int]:
    """
    Update target with source values only if their hash differs.

    ``hash_function`` is called on every present value, ``None`` included.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        hash_function: Function to generate a hash of a value
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, print debug information, if callable, call with
            (key, decision) on each key that is looped over

    Returns:
        Dictionary with statistics about the update operation
    """

    def hash_of(mapping: Mapping[K, V]) -> KeyInfoExtractor:
        return lambda key: hash_function(mapping[key])

    return update_with_policy(
        target,
        source,
        policy=DefaultPolicy.UPDATE_IF_DIFFERENT,
        target_key_info=hash_of(target),
        source_key_info=hash_of(source),
        keys_to_consider=keys_to_consider,
        verbose=verbose,
    )


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
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
) -> dict[str, int]:
    """
    Update target with source values only if source has a newer timestamp.

    Args:
        target: The mapping to be updated (modified in-place)
        source: The mapping containing items to potentially copy to target
        target_timestamp: Function(key) -> timestamp that extracts timestamp from target
        source_timestamp: Function(key) -> timestamp that extracts timestamp from source
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, print debug information, if callable, call with
            (key, decision) on each key that is looped over

    Returns:
        Dictionary with statistics about the update operation
    """

    def _newer_decider(
        key: K,
        target_info: Any,
        source_info: Any,
    ) -> KeyDecision:
        """Decision function based on timestamp comparison (infos are timestamps)."""
        if source_info is MISSING:
            return KeyDecision.SKIP
        if target_info is MISSING:
            return KeyDecision.COPY
        # If either timestamp is None, skip update (can't compare)
        if source_info is None or target_info is None:
            return KeyDecision.SKIP
        try:
            return KeyDecision.COPY if source_info > target_info else KeyDecision.SKIP
        except TypeError:  # can't compare
            return KeyDecision.SKIP

    def _safe(timestamp: Callable[[K], Any]) -> KeyInfoExtractor:
        def info(key: K) -> Any:
            try:
                return timestamp(key)
            except (KeyError, FileNotFoundError, AttributeError, TypeError):
                return None  # can't get a timestamp: skip

        return info

    return update_with_policy(
        target,
        source,
        policy=_newer_decider,
        target_key_info=_safe(target_timestamp),
        source_key_info=_safe(source_timestamp),
        keys_to_consider=keys_to_consider,
        verbose=verbose,
        missing=MISSING,
    )


# Convenience function for file-based stores
@add_as_attribute_of(update_newer, name="files_by_timestamp")
def update_files_by_timestamp(
    target: MutableMapping[K, V],
    source: Mapping[K, V],
    *,
    keys_to_consider: KeysToConsider = _just_source_keys,
    verbose: bool | Callable[[K, KeyDecision], Any] = False,
) -> dict[str, int]:
    """
    Update a target file store with files from a source store based on modification times.

    This is a convenience wrapper around update_newer that uses local_file_timestamp
    to compare file modification times.

    Args:
        target: The target file store to be updated
        source: The source file store containing potential updates
        keys_to_consider: Specific iterable of keys to consider or a callable to make
            this from source and target. The default will consider source keys only.
        verbose: If True, print debug information, if callable, call with
            (key, decision) on each key that is looped over

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
        verbose=verbose,
    )


# ---------------------------------------------------------------------------
# Named entry points for a master / copy pair (companions of xdol.tiering)


def _as_update_stats(stats: dict[str, int]) -> UpdateStats:
    return UpdateStats(**stats)


def _copy_keys(master: Mapping, copy: Mapping):
    return copy.keys()


def backfill(
    *,
    master: MutableMapping[K, V],
    copy: Mapping[K, V],
    keys_to_consider: KeysToConsider = _copy_keys,
) -> UpdateStats:
    """Push to ``master`` the items that only ``copy`` has. Never overwrites.

    Decisions use key presence only: the one value read is ``copy[k]`` for each key
    actually pushed, so nothing is ever downloaded from a cold (e.g. S3) master.
    ``keys_to_consider``: keys, or ``(master, copy) -> keys`` (default: copy's keys).

    >>> master, copy = {'a': 1}, {'a': 9, 'b': 2}
    >>> backfill(copy=copy, master=master).added
    1
    >>> master
    {'a': 1, 'b': 2}
    """
    if callable(keys_to_consider):
        keys_to_consider = keys_to_consider(master, copy)
    return _as_update_stats(
        update_with_policy(
            master,
            copy,
            policy=DefaultPolicy.PREFER_TARGET,
            keys_to_consider=keys_to_consider,
        )
    )


def reconcile(
    *,
    master: Mapping[K, V],
    copy: MutableMapping[K, V],
    key_info_for: KeyInfoFor | None = None,
    policy: DefaultPolicy | UpdateDecider = DefaultPolicy.UPDATE_IF_DIFFERENT,
    keys_to_consider: KeysToConsider = _just_source_keys,
) -> UpdateStats:
    """Update ``copy`` from ``master``: what differs is rewritten, unless ``policy``
    says otherwise.

    :param key_info_for: ``key_info_for(store)`` gives the ``key -> info`` compared for
        that store (e.g. sizes or ETags, to avoid reading values); default: the values
        themselves, any pair that can't be compared counting as different (and, for a
        policy that looks at presence only, nothing is read to decide).
    :param keys_to_consider: keys, or ``(master, copy) -> keys`` (default: master's).

    >>> master, copy = {'a': 1, 'b': 2, 'c': 3}, {'a': 9, 'c': 3}
    >>> s = reconcile(master=master, copy=copy)
    >>> s.updated, s.added, s.unchanged
    (1, 1, 1)
    >>> copy
    {'a': 1, 'c': 3, 'b': 2}
    """
    infos = {}
    if key_info_for is not None:
        infos = dict(
            target_key_info=key_info_for(copy), source_key_info=key_info_for(master)
        )
    return _as_update_stats(
        update_with_policy(
            copy, master, policy=policy, keys_to_consider=keys_to_consider, **infos
        )
    )


@dataclass
class VerifyReport:
    """Result of :func:`verify`: the keys where ``copy`` and ``master`` disagree."""

    only_in_copy: list = field(default_factory=list)
    only_in_master: list = field(default_factory=list)
    mismatched: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.only_in_copy or self.only_in_master or self.mismatched)


_STR_HASH_TAG = b"str:"
_BYTES_HASH_TAG = b"bytes:"
_STR_HASH_ENCODING = "utf-8"
_STR_HASH_ERRORS = "surrogatepass"  # lone surrogates must hash, not raise


def _content_hash(value) -> Any:
    if isinstance(value, str):
        tag = _STR_HASH_TAG
        value = value.encode(_STR_HASH_ENCODING, errors=_STR_HASH_ERRORS)
    elif isinstance(value, (bytes, bytearray, memoryview)):
        tag = _BYTES_HASH_TAG
    else:
        return value
    hasher = hashlib.sha256(tag)
    hasher.update(value)
    return hasher.hexdigest()


def content_hash_key_info(store: Mapping) -> KeyInfoExtractor:
    """A ``key_info_for``: ``key -> content hash`` for ``store``.

    Text and bytes-like values give a sha256 of their content, tagged by kind so
    that ``'a'`` and ``b'a'`` differ. Any other value is returned as is. Hashes
    compare exactly as the values do with ``==`` (``1 == 1.0``, dict order ignored),
    so this is for when a hash, not the value, is what must be kept or sent.

    >>> h = lambda v: content_hash_key_info({'k': v})('k')
    >>> h(b'a') == h(bytearray(b'a')), h(b'a') == h('a')
    (True, False)
    >>> h({'x': 1})
    {'x': 1}
    """
    return lambda key: _content_hash(store[key])


def verify(
    *,
    master: Mapping[K, V],
    copy: Mapping[K, V],
    key_info_for: KeyInfoFor | None = None,
    keys_to_consider: KeysToConsider = union_iter,
) -> VerifyReport:
    """Compare ``copy`` with ``master`` without writing anything.

    :param key_info_for: ``key_info_for(store)`` gives the ``key -> info`` compared for
        that store; pass a cheaper one (size, ETag) to avoid reading values. Default:
        the values themselves, any pair that can't be compared counting as different.
    :param keys_to_consider: keys, or ``(master, copy) -> keys`` (default: the keys of
        both, master's first), each considered once.

    >>> r = verify(master={'a': 1, 'b': 20, 'd': 4}, copy={'a': 1, 'b': 2, 'c': 3})
    >>> r.only_in_copy, r.only_in_master, r.mismatched
    (['c'], ['d'], ['b'])
    """
    if callable(keys_to_consider):
        keys_to_consider = keys_to_consider(master, copy)
    if key_info_for is None:
        key_info_for = lambda store: partial(_key_info_from_mapping, store)

    def deferred(store: Mapping) -> KeyInfoExtractor:
        """Info as a thunk: read only for a key both stores have."""
        info = key_info_for(store)
        return lambda key: lambda: info(key)

    report = VerifyReport()

    def record(key, copy_info, master_info) -> KeyDecision:
        if master_info is MISSING:
            if copy_info is not MISSING:  # (a requested key may be in neither)
                report.only_in_copy.append(key)
        elif copy_info is MISSING:
            report.only_in_master.append(key)
        elif _differ(copy_info(), master_info()):
            report.mismatched.append(key)
        return KeyDecision.SKIP  # verify never writes

    update_with_policy(
        copy,
        master,
        policy=record,
        keys_to_consider=union_iter(keys_to_consider),
        target_key_info=deferred(copy),
        source_key_info=deferred(master),
        missing=MISSING,
    )
    return report
