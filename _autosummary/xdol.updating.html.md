# xdol.updating

Controlled updates of one mapping from another, with customizable policies.

`update_with_policy` decides, key by key, whether to copy a source item into a
target mapping. It goes beyond `dict.update()` with:

- built-in policies (always update, update if different, prefer target, prefer source, an alias of always update)
- custom decision functions and custom “key info” extractors (compare sizes,
  hashes or timestamps instead of reading values)
- statistics about what was examined, updated, added, unchanged or deleted
- specialized entry points (content-hash comparison, timestamps, local files)

A last section names the operations on a master / copy pair (companions of
`xdol.tiering`): `backfill` (push to master what only the copy has),
`reconcile` (update the copy from master) and `verify` (compare without
writing).

Custom update logic with a hash function:

```pycon
>>> target = {"a": "hello", "b": "world"}
>>> source = {"a": "hello!", "c": "python"}
>>> update_by_content_hash(target, source, hash_function=lambda x: len(x))
{'examined': 2, 'updated': 1, 'added': 1, 'unchanged': 0, 'deleted': 0}
>>> target
{'a': 'hello!', 'b': 'world', 'c': 'python'}
```

Using convenience methods:

```pycon
>>> update_with_policy.missing_only(target, source)
{'examined': 2, 'updated': 0, 'added': 0, 'unchanged': 2, 'deleted': 0}
>>> target
{'a': 'hello!', 'b': 'world', 'c': 'python'}
```

### Module Attributes

| [`KeyInfoFor`](#xdol.updating.KeyInfoFor)   | the `key -> info` extractor for `store`.                         |
|---------------------------------------------------------------|------------------------------------------------------------------|
| [`MISSING`](#xdol.updating.MISSING)      | Info given to a decider for a key that is absent from a mapping. |

### Functions

| [`add_as_attribute_of`](#xdol.updating.add_as_attribute_of)(obj[, name])                 | Decorator to add the function as an attribute of the object.                            |
|---------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| [`backfill`](#xdol.updating.backfill)(\*, master, copy[, keys_to_consider])   | Push to `master` the items that only `copy` has.                                        |
| [`content_hash_key_info`](#xdol.updating.content_hash_key_info)(store)                     | A `key_info_for`: `key -> content hash` for `store`.                                    |
| [`local_file_timestamp`](#xdol.updating.local_file_timestamp)(store, key)                 | Get the modified timestamp of a file in a local file store.                             |
| [`print_all_but_skips`](#xdol.updating.print_all_but_skips)(key, decision, \*[, ...])    | Print all decisions except SKIP.                                                        |
| [`reconcile`](#xdol.updating.reconcile)(\*, master, copy[, key_info_for, ...]) | Update `copy` from `master`: what differs is rewritten, unless `policy` says otherwise. |
| [`union_iter`](#xdol.updating.union_iter)(\*iterables)                          | Generator yielding unique hashable items from an iterable of iterables.                 |
| [`update_all`](#xdol.updating.update_all)(target, source, \*[, ...])            | Update target with all source values, equivalent to dict.update().                      |
| [`update_by_content_hash`](#xdol.updating.update_by_content_hash)(target, source, \*, ...)  | Update target with source values only if their hash differs.                            |
| [`update_files_by_timestamp`](#xdol.updating.update_files_by_timestamp)(target, source, \*)    | Update a target file store with files from a source store based on modification times.  |
| [`update_if_different`](#xdol.updating.update_if_different)(target, source, \*[, ...])   | Update target with source values only if they differ.                                   |
| [`update_missing_only`](#xdol.updating.update_missing_only)(target, source, \*[, ...])   | Update target with source values only for keys not in target.                           |
| [`update_newer`](#xdol.updating.update_newer)(target, source, \*, ...[, ...])     | Update target with source values only if source has a newer timestamp.                  |
| [`update_with_policy`](#xdol.updating.update_with_policy)(target, source, \*[, ...])    | Update a target mapping with values from a source mapping using a customizable policy.  |
| [`verify`](#xdol.updating.verify)(\*, master, copy[, key_info_for, ...])    | Compare `copy` with `master` without writing anything.                                  |

### Classes

| [`DefaultPolicy`](#xdol.updating.DefaultPolicy)(\*values)                           | Standard policies for updating mappings.                                                                          |
|----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| [`KeyDecision`](#xdol.updating.KeyDecision)(\*values)                             | Decisions for individual keys during update.                                                                      |
| [`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor)(\*args, \*\*kwargs)              | Protocol for functions that extract comparison information from values.                                           |
| [`NotRetrieved`](#xdol.updating.NotRetrieved)()                                    | Sentinel object indicating a value that hasn't been retrieved yet.                                                |
| [`StringEnum`](#xdol.updating.StringEnum)(new_class_name, /, names, \*[, ...])   | Enum where members are also (and must be) strings.                                                                |
| [`UpdateDecider`](#xdol.updating.UpdateDecider)(\*args, \*\*kwargs)                 | Protocol for functions that decide whether to update a key.                                                       |
| [`UpdateStats`](#xdol.updating.UpdateStats)([examined, updated, added, ...])      | Statistics for an update operation.                                                                               |
| [`VerifyReport`](#xdol.updating.VerifyReport)([only_in_copy, only_in_master, ...]) | Result of [`verify()`](#xdol.updating.verify): the keys where `copy` and `master` disagree. |

### *class* xdol.updating.DefaultPolicy(\*values)

Bases: [`StringEnum`](#xdol.updating.StringEnum)

Standard policies for updating mappings.

### *class* xdol.updating.KeyDecision(\*values)

Bases: [`StringEnum`](#xdol.updating.StringEnum)

Decisions for individual keys during update.

### *class* xdol.updating.KeyInfoExtractor(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

Protocol for functions that extract comparison information from values.

### xdol.updating.KeyInfoFor

the `key -> info` extractor for `store`.

* **Type:**
  `key_info_for(store)`

alias of [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)], [`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor)]

### xdol.updating.MISSING *= MISSING*

Info given to a decider for a key that is absent from a mapping. Unlike `None`,
it cannot be mistaken for a key whose value or info is `None`.

### *class* xdol.updating.NotRetrieved

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Sentinel object indicating a value that hasn’t been retrieved yet.

### *class* xdol.updating.StringEnum(new_class_name, , names, , module=None, qualname=None, type=None, start=1, boundary=None)

Bases: [`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Enum where members are also (and must be) strings.

### *class* xdol.updating.UpdateDecider(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

Protocol for functions that decide whether to update a key.

### *class* xdol.updating.UpdateStats(examined=0, updated=0, added=0, unchanged=0, deleted=0)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Statistics for an update operation.

#### as_dict()

Convert stats to a dictionary.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]

### *class* xdol.updating.VerifyReport(only_in_copy=list(), only_in_master=list(), mismatched=list())

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Result of [`verify()`](#xdol.updating.verify): the keys where `copy` and `master` disagree.

### xdol.updating.add_as_attribute_of(obj, name=None)

Decorator to add the function as an attribute of the object.

* **Parameters:**
  * **obj** – The object to which the function will be added
  * **name** – The name of the attribute to be added, if None, uses the function name
* **Returns:**
  The decorator function

### Examples

```pycon
>>> def foo():
...     return "bar"
...
>>> @add_as_attribute_of(foo)
... def baz():
...     return "qux"
...
>>> foo.baz()
'qux'
```

### xdol.updating.backfill(\*, master, copy, keys_to_consider=<function \_copy_keys>)

Push to `master` the items that only `copy` has. Never overwrites.

Decisions use key presence only: the one value read is `copy[k]` for each key
actually pushed, so nothing is ever downloaded from a cold (e.g. S3) master.
`keys_to_consider`: keys, or `(master, copy) -> keys` (default: copy’s keys).

```pycon
>>> master, copy = {'a': 1}, {'a': 9, 'b': 2}
>>> backfill(copy=copy, master=master).added
1
>>> master
{'a': 1, 'b': 2}
```

* **Return type:**
  [`UpdateStats`](#xdol.updating.UpdateStats)

### xdol.updating.content_hash_key_info(store)

A `key_info_for`: `key -> content hash` for `store`.

Text and bytes-like values give a sha256 of their content, tagged by kind so
that `'a'` and `b'a'` differ. Any other value is returned as is. Hashes
compare exactly as the values do with `==` (`1 == 1.0`, dict order ignored),
so this is for when a hash, not the value, is what must be kept or sent.

```pycon
>>> h = lambda v: content_hash_key_info({'k': v})('k')
>>> h(b'a') == h(bytearray(b'a')), h(b'a') == h('a')
(True, False)
>>> h({'x': 1})
{'x': 1}
```

* **Return type:**
  [`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor)

### xdol.updating.local_file_timestamp(store, key)

Get the modified timestamp of a file in a local file store.

Uses inner_most_key to handle relative paths correctly, resolving to
the full path before getting the timestamp.

* **Parameters:**
  * **store** – A mapping whose keys resolve to file paths
  * **key** – A key in the store
* **Returns:**
  The modification timestamp of the file
* **Return type:**
  [`float`](https://docs.python.org/3/builtins/functions.html#float)

### xdol.updating.print_all_but_skips(key, decision, \*, print_func=<built-in function print>)

Print all decisions except SKIP.

### xdol.updating.reconcile(\*, master, copy, key_info_for=None, policy=DefaultPolicy.UPDATE_IF_DIFFERENT, keys_to_consider=<function \_just_source_keys>)

Update `copy` from `master`: what differs is rewritten, unless `policy`
says otherwise.

* **Parameters:**
  * **key_info_for** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)], [`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – `key_info_for(store)` gives the `key -> info` compared for
    that store (e.g. sizes or ETags, to avoid reading values); default: the values
    themselves, any pair that can’t be compared counting as different (and, for a
    policy that looks at presence only, nothing is read to decide).
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – keys, or `(master, copy) -> keys` (default: master’s).
* **Return type:**
  [`UpdateStats`](#xdol.updating.UpdateStats)

```pycon
>>> master, copy = {'a': 1, 'b': 2, 'c': 3}, {'a': 9, 'c': 3}
>>> s = reconcile(master=master, copy=copy)
>>> s.updated, s.added, s.unchanged
(1, 1, 1)
>>> copy
{'a': 1, 'c': 3, 'b': 2}
```

### xdol.updating.union_iter(\*iterables)

Generator yielding unique hashable items from an iterable of iterables.

* **Parameters:**
  **iterables** ([`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`T`]) – An iterable where each element is itself an iterable of hashable items.
* **Yields:**
  `T` – Unique items from the nested iterables, in order of first appearance.
* **Return type:**
  [*Generator*](https://docs.python.org/3/library/collections.abc.html#collections.abc.Generator)[*T*, *None*, *None*]

### Example

```pycon
>>> list(union_iter(['a', 'b'], ['b', 'c']))
['a', 'b', 'c']
>>> list(union_iter({'a': 1, 'b': 2}.keys(), {'b': 3, 'c': 4}.values()))
['a', 'b', 3, 4]
```

### xdol.updating.update_all(target, source, \*, keys_to_consider=<function \_just_source_keys>, verbose=False)

Update target with all source values, equivalent to dict.update().

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The mapping to be updated (modified in-place)
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The mapping containing items to potentially copy to target
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, print debug information, if callable, call with
    (key, decision) on each key that is looped over
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### xdol.updating.update_by_content_hash(target, source, \*, hash_function, keys_to_consider=<function \_just_source_keys>, verbose=False)

Update target with source values only if their hash differs.

`hash_function` is called on every present value, `None` included.

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The mapping to be updated (modified in-place)
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The mapping containing items to potentially copy to target
  * **hash_function** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`V`], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – Function to generate a hash of a value
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, print debug information, if callable, call with
    (key, decision) on each key that is looped over
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### xdol.updating.update_files_by_timestamp(target, source, \*, keys_to_consider=<function \_just_source_keys>, verbose=False)

Update a target file store with files from a source store based on modification times.

This is a convenience wrapper around update_newer that uses local_file_timestamp
to compare file modification times.

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The target file store to be updated
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The source file store containing potential updates
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, print debug information, if callable, call with
    (key, decision) on each key that is looped over
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### xdol.updating.update_if_different(target, source, \*, key_info=None, target_key_info=None, source_key_info=None, keys_to_consider=<function \_just_source_keys>, verbose=False)

Update target with source values only if they differ.

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The mapping to be updated (modified in-place)
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The mapping containing items to potentially copy to target
  * **key_info** – (Deprecated) Function to extract comparison info from values
  * **target_key_info** – Function to extract comparison info from target keys
  * **source_key_info** – Function to extract comparison info from source keys
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, print debug information, if callable, call with
    (key, decision) on each key that is looped over
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### xdol.updating.update_missing_only(target, source, \*, keys_to_consider=<function \_just_source_keys>, verbose=False)

Update target with source values only for keys not in target.

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The mapping to be updated (modified in-place)
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The mapping containing items to potentially copy to target
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, print debug information, if callable, call with
    (key, decision) on each key that is looped over
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### xdol.updating.update_newer(target, source, \*, target_timestamp, source_timestamp, keys_to_consider=<function \_just_source_keys>, verbose=False)

Update target with source values only if source has a newer timestamp.

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The mapping to be updated (modified in-place)
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The mapping containing items to potentially copy to target
  * **target_timestamp** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – Function(key) -> timestamp that extracts timestamp from target
  * **source_timestamp** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – Function(key) -> timestamp that extracts timestamp from source
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, print debug information, if callable, call with
    (key, decision) on each key that is looped over
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### xdol.updating.update_with_policy(target, source, \*, policy=DefaultPolicy.UPDATE_IF_DIFFERENT, target_key_info=None, source_key_info=None, keys_to_consider=<function \_just_source_keys>, verbose=False, missing=None)

Update a target mapping with values from a source mapping using a customizable policy.

* **Parameters:**
  * **target** ([`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[`K`, `V`]) – The mapping to be updated (modified in-place)
  * **source** ([`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[`K`, `V`]) – The mapping containing items to potentially copy to target
  * **policy** ([`DefaultPolicy`](#xdol.updating.DefaultPolicy) | [`str`](https://docs.python.org/3/builtins/stdtypes.html#str) | [`UpdateDecider`](#xdol.updating.UpdateDecider)) – Either a DefaultPolicy enum value or a custom decision function
  * **target_key_info** ([`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Function to extract comparison info from target keys
  * **source_key_info** ([`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Function to extract comparison info from source keys
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – Specific iterable of keys to consider or a callable to make
    this from source and target. The default will consider source keys only.
  * **verbose** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool) | [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[`K`, [`KeyDecision`](#xdol.updating.KeyDecision)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – If True, will print debug information, if callable, call with
    (key, decision) on each key that is looped over
  * **missing** ([`Any`](https://docs.python.org/3/library/typing.html#typing.Any)) – What a *custom* decider gets as the info of a key absent from a
    mapping. `None` (the default, for now) can’t be told from a present key
    whose info is `None`; pass [`MISSING`](#xdol.updating.MISSING) to tell them apart (i2mint/xdol#21).
    The standard policies always use `MISSING` and, when they look at
    presence only, read no value or info to decide.
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`int`](https://docs.python.org/3/builtins/functions.html#int)]
* **Returns:**
  Dictionary with statistics about the update operation

### Examples

```pycon
>>> target = {"a": 1, "b": 2}
>>> source = {"a": 10, "c": 30}
>>> update_with_policy(target, source)
{'examined': 2, 'updated': 1, 'added': 1, 'unchanged': 0, 'deleted': 0}
>>> target
{'a': 10, 'b': 2, 'c': 30}
```

```pycon
>>> # Using PREFER_TARGET policy
>>> target = {"a": 1, "b": 2}
>>> source = {"a": 10, "c": 30}
>>> update_with_policy(target, source, policy=DefaultPolicy.PREFER_TARGET)
{'examined': 2, 'updated': 0, 'added': 1, 'unchanged': 1, 'deleted': 0}
>>> target
{'a': 1, 'b': 2, 'c': 30}
```

### xdol.updating.verify(\*, master, copy, key_info_for=None, keys_to_consider=<function union_iter>)

Compare `copy` with `master` without writing anything.

* **Parameters:**
  * **key_info_for** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)], [`KeyInfoExtractor`](#xdol.updating.KeyInfoExtractor)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – `key_info_for(store)` gives the `key -> info` compared for
    that store; pass a cheaper one (size, ETag) to avoid reading values. Default:
    the values themselves, any pair that can’t be compared counting as different.
  * **keys_to_consider** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Mapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping), [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[`K`]]]) – keys, or `(master, copy) -> keys` (default: the keys of
    both, master’s first), each considered once.
* **Return type:**
  [`VerifyReport`](#xdol.updating.VerifyReport)

```pycon
>>> r = verify(master={'a': 1, 'b': 20, 'd': 4}, copy={'a': 1, 'b': 2, 'c': 3})
>>> r.only_in_copy, r.only_in_master, r.mismatched
(['c'], ['d'], ['b'])
```
