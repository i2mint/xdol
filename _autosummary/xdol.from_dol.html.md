# xdol.from_dol

Tools that left `dol`’s top-level namespace for xdol (dol issue #145).

`dol` keeps its core small. These names were exported at `dol`’s top level but used
nowhere inside dol, so their public home is now xdol: import them from here (or from
`xdol` directly). `dol.<name>` still works for one release, with a
`DeprecationWarning` pointing here.

`kvhead` and `ihead` are defined here. The others are, for now, re-exported from the
dol submodule that still defines them (so there is one definition of each); when dol
removes them, their definitions move into this module.

```pycon
>>> from xdol import kvhead, ihead, igroupby
>>> kvhead({'a': 1, 'b': 2})
('a', 1)
>>> ihead('abc', 2)
['a', 'b']
>>> dict(igroupby(['apple', 'avocado', 'banana'], key=lambda w: w[0]))
{'a': ['apple', 'avocado'], 'b': ['banana']}
```

### Functions

| [`igroupby`](#xdol.from_dol.igroupby)(items, key[, val, group_factory, ...])    | The generator version of dol groupby.                                                                           |
|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| [`regroupby`](#xdol.from_dol.regroupby)(items, \*key_funcs, \*\*named_key_funcs) | Recursive groupby.                                                                                              |
| [`ihead`](#xdol.from_dol.ihead)(store[, n])                                  | Get the first item of an iterable, or a list of the first n items                                               |
| [`kvhead`](#xdol.from_dol.kvhead)(store[, n])                                 | Get the first item of a kv store, or a list of the first n items                                                |
| [`mk_store_from_path_format_store_cls`](#xdol.from_dol.mk_store_from_path_format_store_cls)([store, ...])  | Wrap a store (instance or class) that uses string keys to make it into a store that uses a specific key format. |
| [`redirect_getattr_to_getitem`](#xdol.from_dol.redirect_getattr_to_getitem)([cls, ...])            | A mapping decorator that redirects attribute access to \_\_getitem_\_.                                          |
| [`trace_getitem`](#xdol.from_dol.trace_getitem)(store, k[, layer_attrs])             | A generator of layered steps to inspect a store.                                                                |
| [`to_zip_file`](#xdol.from_dol.to_zip_file)(b, zip_filepath[, filename, ...])      | Zip input bytes and save to a single-file zip file.                                                             |

### Classes

| [`AttributeMutableMapping`](#xdol.from_dol.AttributeMutableMapping)                     | A mutable mapping that provides both attribute and dictionary-style access.                                                                           |
|----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`ExplicitKeyMap`](#xdol.from_dol.ExplicitKeyMap)(\*[, key_of_id, id_of_key])  | A key mapper given as explicit `key_of_id`/`id_of_key` dicts (one is enough; the other is derived, and both are checked to be inverse of each other). |
| [`SequenceKvReader`](#xdol.from_dol.SequenceKvReader)(sequence[, key, val, ...]) | A KvReader that sources itself in an iterable of elements from which keys and values will be extracted and grouped by key.                            |

### *class* xdol.from_dol.AttributeMutableMapping

Bases: `AttributeMapping`, [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]

A mutable mapping that provides both attribute and dictionary-style access.

Extends AttributeMapping with mutation capabilities,
ensuring proper error handling and protocol compliance.

### Examples

```pycon
>>> ns = AttributeMutableMapping(apple=1, banana=2)
>>> ns.apple
1
>>> ns['banana']
2
>>> ns['cherry'] = 3
>>> ns.cherry
3
>>> list(ns)
['apple', 'banana', 'cherry']
>>> len(ns)
3
>>> 'apple' in ns
True
>>> del ns['banana']
>>> 'banana' in ns
False
```

### *class* xdol.from_dol.ExplicitKeyMap(, key_of_id=None, id_of_key=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

A key mapper given as explicit `key_of_id`/`id_of_key` dicts (one is enough;
the other is derived, and both are checked to be inverse of each other).
Provides the `_key_of_id`/`_id_of_key` methods that `kv_wrap` looks for.

### *class* xdol.from_dol.SequenceKvReader(sequence, key=None, val=None, val_postproc=<class 'list'>)

Bases: `KvReader`

A KvReader that sources itself in an iterable of elements from which keys and values
will be extracted and grouped by key.

```pycon
>>> docs = [{'_id': 0, 's': 'a', 'n': 1},
...  {'_id': 1, 's': 'b', 'n': 2},
...  {'_id': 2, 's': 'b', 'n': 3}]
>>>
```

Out of the box, SequenceKvReader gives you enumerated integer indices as keys,
and the sequence items as is, as vals

```pycon
>>> s = SequenceKvReader(docs)
>>> list(s)
[0, 1, 2]
>>> s[1]
{'_id': 1, 's': 'b', 'n': 2}
>>> assert s.get('not_a_key') is None
```

You can make it more interesting by specifying a val function to compute the vals
from the sequence elements

```pycon
>>> s = SequenceKvReader(docs, val=lambda x: (x['_id'] + x['n']) * x['s'])
>>> assert list(s) == [0, 1, 2]  # as before
>>> list(s.values())
['a', 'bbb', 'bbbbb']
```

But where it becomes more useful is when you specify a key as well.
SequenceKvReader will then compute the keys with that function, group them,
and return as the value, the list of sequence elements that match that key.

```pycon
>>> s = SequenceKvReader(docs,
...         key=lambda x: x['s'],
...         val=lambda x: {k: x[k] for k in x.keys() - {'s'}})
>>> assert list(s) == ['a', 'b']
>>> assert s['a'] == [{'_id': 0, 'n': 1}]
>>> assert s['b'] == [{'_id': 1, 'n': 2}, {'_id': 2, 'n': 3}]
```

The cannonical form of key and val is a function, but if you specify a str, int,
or iterable thereof,
SequenceKvReader will make an itemgetter function from it, for your convenience.

```pycon
>>> s = SequenceKvReader(docs, key='_id')
>>> assert list(s) == [0, 1, 2]
>>> assert s[1] == [{'_id': 1, 's': 'b', 'n': 2}]
```

The `val_postproc` argument is `list` by default, but what if we don’t specify
any?
Well then you’ll get an unconsumed iterable of matches

```pycon
>>> s = SequenceKvReader(docs, key='_id', val_postproc=None)
>>> assert isinstance(s[1], Iterable)
```

The `val_postproc` argument specifies what to apply to this iterable of matches.
For example, you can specify `val_postproc=next` to simply get the first matched
element:

```pycon
>>> s = SequenceKvReader(docs, key='_id', val_postproc=next)
>>> assert list(s) == [0, 1, 2]
>>> assert s[1] == {'_id': 1, 's': 'b', 'n': 2}
```

We got the whole dict there. What if we just want we didn’t want the \_id, which is
used by the key, in our val?

```pycon
>>> from functools import partial
>>> all_but_s = partial(exclusive_subdict, exclude=['s'])
>>> s = SequenceKvReader(docs, key='_id', val=all_but_s, val_postproc=next)
>>> assert list(s) == [0, 1, 2]
>>> assert s[1] == {'_id': 1, 'n': 2}
```

Suppose we want to have the pair of (‘_id’, ‘n’) values as a key, and only ‘s’
as a value…

```pycon
>>> s = SequenceKvReader(docs, key=('_id', 'n'), val='s', val_postproc=next)
>>> assert list(s) == [(0, 1), (1, 2), (2, 3)]
>>> assert s[1, 2] == 'b'
```

But remember that using `val_postproc=next` will only give you the first match
as a val.

```pycon
>>> s = SequenceKvReader(docs, key='s', val=all_but_s, val_postproc=next)
>>> assert list(s) == ['a', 'b']
>>> assert s['a'] == {'_id': 0, 'n': 1}
>>> assert s['b'] == {'_id': 1, 'n': 2}   # note that only the first match is returned.
```

If you do want to only grab the first match, but want to additionally assert
that there is no more than one,
you can specify this with `val_postproc=unique_element`:

```pycon
>>> s = SequenceKvReader(docs, key='s', val=all_but_s, val_postproc=unique_element)
>>> assert s['a'] == {'_id': 0, 'n': 1}
>>> # The following should raise an exception since there's more than one match
>>> s['b']
Traceback (most recent call last):
  ...
sources.NotUnique: iterator had more than one element
```

### xdol.from_dol.igroupby(items, key, val=None, group_factory=<class 'list'>, group_release_cond=<function <lambda>>, release_remainding=True, append_to_group_items=<method 'append' of 'list' objects>, grouper_mapping=<class 'collections.defaultdict'>)

The generator version of dol groupby.
Groups items according to group keys updated from those items through the given `key` function (mapping an item to its group key),
yielding the groups according to a logic defined by `group_release_cond`

* **Parameters:**
  * **items** ([`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]) – iterable of items
  * **key** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)], [`Hashable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Hashable)]) – The function that computes a key from an item. Needs to return a hashable.
  * **val** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – An optional function that computes a val from an item. If not given, the item itself will be taken.
  * **group_factory** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[], [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]]) – The function to make new (empty) group objects and accumulate group items.
    group_items = group_collector() will be called to make a new empty group collection
    group_items.append(x) will be called to add x to that collection
    The default is `list`
  * **group_release_cond** ([`Union`](https://docs.python.org/3/library/typing.html#typing.Union)[[`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Hashable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Hashable), [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]], [`bool`](https://docs.python.org/3/builtins/functions.html#bool)], [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict), [`Hashable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Hashable), [`Iterable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]], [`bool`](https://docs.python.org/3/builtins/functions.html#bool)]]) – A boolean function that will be applied, at every iteration,
    to the accumulated items of the group that was just updated,
    and determines (if True) if the (group_key, group_items) should be yielded.
    The default is False, which results in
    `lambda group_key, group_items: False` being used.
  * **release_remainding** – Once the input items have been consumed, there may still be some
    items in the grouping “cache”. `release_remainding` is a boolean that indicates whether
    the contents of this cache should be released or not.
* **Yields:**
  `(group_key, items_in_that_group)` pairs

The following will group numbers according to their parity (0 for even, 1 for odd),
releasing a list of numbers collected when that list reaches length 3:

```pycon
>>> g = igroupby(items=range(11),
...             key=lambda x: x % 2,
...             group_release_cond=lambda k, v: len(v) == 3)
>>> list(g)
[(0, [0, 2, 4]), (1, [1, 3, 5]), (0, [6, 8, 10]), (1, [7, 9])]
```

If we specify `release_remainding=False` though, we won’t get

```pycon
>>> g = igroupby(items=range(11),
...             key=lambda x: x % 2,
...             group_release_cond=lambda k, v: len(v) == 3,
...             release_remainding=False)
>>> list(g)
[(0, [0, 2, 4]), (1, [1, 3, 5]), (0, [6, 8, 10])]
```

# >>> grps = partial(igroupby, group_release_cond=False, release_remainding=True)

Below we show that, with the default `group_release_cond = lambda k, v: False`
and release_remainding=True\`\` we have `dict(igroupby(...)) == groupby(...)`

```pycon
>>> from functools import partial
>>> from dol import groupby
>>>
>>> kws = dict(items=range(11), key=lambda x: x % 3)
>>> assert (dict(igroupby(**kws)) == groupby(**kws)
...         == {0: [0, 3, 6, 9], 1: [1, 4, 7, 10], 2: [2, 5, 8]})
>>>
>>> tokens = ['the', 'fox', 'is', 'in', 'a', 'box']
>>> kws = dict(items=tokens, key=len)
>>> assert (dict(igroupby(**kws)) == groupby(**kws)
...         == {3: ['the', 'fox', 'box'], 2: ['is', 'in'], 1: ['a']})
>>>
>>> key_map = {1: 'one', 2: 'two'}
>>> kws.update(key=lambda x: key_map.get(len(x), 'more'))
>>> assert (dict(igroupby(**kws)) == groupby(**kws)
...         == {'more': ['the', 'fox', 'box'], 'two': ['is', 'in'], 'one': ['a']})
>>>
>>> stopwords = {'the', 'in', 'a', 'on'}
>>> kws.update(key=lambda w: w in stopwords)
>>> assert (dict(igroupby(**kws)) == groupby(**kws)
...         == {True: ['the', 'in', 'a'], False: ['fox', 'is', 'box']})
>>> kws.update(key=lambda w: ['words', 'stopwords'][int(w in stopwords)])
>>> assert (dict(igroupby(**kws)) == groupby(**kws)
...         == {'stopwords': ['the', 'in', 'a'], 'words': ['fox', 'is', 'box']})
```

### xdol.from_dol.ihead(store, n=1)

Get the first item of an iterable, or a list of the first n items

```pycon
>>> ihead(iter('abc'))
'a'
>>> ihead(iter('')) is None
True
```

### xdol.from_dol.kvhead(store, n=1)

Get the first item of a kv store, or a list of the first n items

```pycon
>>> kvhead({'a': 1, 'b': 2}, 5)
[('a', 1), ('b', 2)]
>>> kvhead({}) is None
True
```

### xdol.from_dol.mk_store_from_path_format_store_cls(store=None, \*, subpath='', store_cls_kwargs=None, key_type=<function namedtuple>, keymap=<class 'dol.naming.StrTupleDict'>, keymap_kwargs=None, name=None, \_\_module_\_=None, \_\_name_\_=None, \_\_qualname_\_=None, \_\_doc_\_=None, \_\_annotations_\_=None, \_\_defaults_\_=None, \_\_kwdefaults_\_=None)

Wrap a store (instance or class) that uses string keys to make it into a store that uses a specific key format.

* **Parameters:**
  * **store** – The instance or class to wrap
  * **subpath** – The subpath (defining the subset of the data pointed at by the URI
  * **store_cls_kwargs** – # if store is a class, the kwargs that you would have given the store_cls to make itself
  * **key_type** – The key type you want to interface with: `dict`, `tuple`,
    `namedtuple`, `str`, or one of those names as a string
  * **keymap** – # the keymap instance or class you want to use to map keys
  * **keymap_kwargs** – # if keymap is a cls, the kwargs to give it (besides the subpath)
  * **name** – The name to give the class the function will make here
* **Returns:**
  An instance of a wrapped class

### Example

```python
# Get a (session, bt) indexed LocalJsonStore
s = mk_store_from_path_format_store_cls(LocalJsonStore,
                                               os.path.join(root_dir, 'd'),
                                               subpath='{session}/d/{bt}',
                                               keymap_kwargs=dict(process_info_dict={'session': int, 'bt': int}))
```

### xdol.from_dol.redirect_getattr_to_getitem(cls=None, , keys_have_priority_over_attributes=False, \_\_module_\_=None, \_\_name_\_=None, \_\_qualname_\_=None, \_\_doc_\_=None, \_\_annotations_\_=None, \_\_defaults_\_=None, \_\_kwdefaults_\_=None)

A mapping decorator that redirects attribute access to \_\_getitem_\_.

#### WARNING
This decorator will make your class un-pickleable.

* **Parameters:**
  **keys_have_priority_over_attributes** – If True, keys will have priority over existing attributes.

```pycon
>>> @redirect_getattr_to_getitem
... class MyDict(dict):
...     pass
>>> d = MyDict(a=1, b=2)
>>> d.a
1
>>> d.b
2
>>> list(d)
['a', 'b']
```

### xdol.from_dol.regroupby(items, \*key_funcs, \*\*named_key_funcs)

Recursive groupby. Applies the groupby function recursively, using a sequence of key functions.

#### NOTE
The named_key_funcs argument names don’t have any external effect.

They just give a name to the key function, for code reading clarity purposes.

#### SEE ALSO
groupby, itertools.groupby, and dol.source.SequenceKvReader

```pycon
>>> # group by how big the number is, then by it's mod 3 value
>>> # note that named_key_funcs argument names doesn't have any external effect (but give a name to the function)
>>> regroupby([1, 2, 3, 4, 5, 6, 7], lambda x: 'big' if x > 5 else 'small', mod3=lambda x: x % 3)
{'small': {1: [1, 4], 2: [2, 5], 0: [3]}, 'big': {0: [6], 1: [7]}}
>>>
>>> tokens = ['the', 'fox', 'is', 'in', 'a', 'box']
>>> stopwords = {'the', 'in', 'a', 'on'}
>>> word_category = lambda x: 'stopwords' if x in stopwords else 'words'
>>> regroupby(tokens, word_category, len)
{'stopwords': {3: ['the'], 2: ['in'], 1: ['a']}, 'words': {3: ['fox', 'box'], 2: ['is']}}
>>> regroupby(tokens, len, word_category)
{3: {'stopwords': ['the'], 'words': ['fox', 'box']}, 2: {'words': ['is'], 'stopwords': ['in']}, 1: {'stopwords': ['a']}}
```

### xdol.from_dol.to_zip_file(b, zip_filepath, filename=None, , compression=8, allow_overwrites=True, pwd=None, encoding='utf-8')

Zip input bytes and save to a single-file zip file.

* **Parameters:**
  * **b** ([`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes) | [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Input bytes or string
  * **zip_filepath** – zip filepath to save the zipped input to
  * **filename** – The name/path of the zip entry we want to save to
  * **encoding** – In case the input is str, the encoding to use to convert to bytes

### xdol.from_dol.trace_getitem(store, k, layer_attrs=('store',))

A generator of layered steps to inspect a store.

* **Parameters:**
  * **store** – An instance that has the base.Store interface
  * **k** – A key
  * **layer_attrs** – The attribute names that should be checked to get the next layer.
* **Returns:**
  A generator of (layer, method, value)

We start with a small dict:

```pycon
>>> d = {'a.num': '1000', 'b.num': '2000'}
```

Now let’s add layers to it. For example, with wrap_kvs:

```pycon
>>> from dol.trans import wrap_kvs
```

Say that we want the interface to not see the `'.num'` strings, and deal with numerical values, not strings.

```pycon
>>> s = wrap_kvs(d,
...              key_decoder=lambda x: x[:-len('.num')],
...              key_encoder=lambda x: x + '.num',
...              value_decoder=lambda x: int(x),
...              value_encoder=lambda x: str(x)
...             )
>>>
```

Oh, and we want the interface to display upper case keys.

```pycon
>>> ss = wrap_kvs(s,
...              key_decoder=lambda x: x.upper(),
...              key_encoder=lambda x: x.lower(),
...             )
```

And we want the numerical unit to be the kilo (that’s 1000):

```pycon
>>> sss = wrap_kvs(ss,
...                value_decoder=lambda x: x / 1000,
...                value_encoder=lambda x: x * 1000
...               )
>>>
>>> dict(sss.items())
{'A': 1.0, 'B': 2.0}
```

Well, if we had bugs, we’d like to inspect the various layers, and how they transform the data.

```python
# Here's how to do that:

# >>> for layer, method, value in trace_getitem(sss, 'A'):
# ...     print(layer, method, value)
# ...
# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
# NameError: name 'trace_getitem' is not defined
```

```pycon
>>> from dol.dig import trace_getitem
>>>
>>> for layer, method, value in trace_getitem(sss, 'A'):
...     print(layer, method, value)
...
{'a.num': '1000', 'b.num': '2000'} _id_of_key A
{'a.num': '1000', 'b.num': '2000'} _id_of_key A
{'a.num': '1000', 'b.num': '2000'} _id_of_key a
{'a.num': '1000', 'b.num': '2000'} _id_of_key a
{'a.num': '1000', 'b.num': '2000'} _id_of_key a.num
{'a.num': '1000', 'b.num': '2000'} _id_of_key a.num
{'a.num': '1000', 'b.num': '2000'} __getitem__ 1000
{'a.num': '1000', 'b.num': '2000'} _obj_of_data 1000
{'a.num': '1000', 'b.num': '2000'} _obj_of_data 1000
{'a.num': '1000', 'b.num': '2000'} _obj_of_data 1000
{'a.num': '1000', 'b.num': '2000'} _obj_of_data 1000
{'a.num': '1000', 'b.num': '2000'} _obj_of_data 1000
{'a.num': '1000', 'b.num': '2000'} _obj_of_data 1.0
```
