# xdol.pystores

Stores for python objects

### Functions

| `bytes_decoder`(x)   |    |
|----------------------|----|

### Classes

| [`PkgFilesReader`](#xdol.pystores.PkgFilesReader)                         |                                                                |
|-----------------------------------------------------------------------------------------|----------------------------------------------------------------|
| [`PkgReader`](#xdol.pystores.PkgReader)(rootdir[, subpath, ...])     | By store we mean key-value store.                              |
| [`PyFilesBytes`](#xdol.pystores.PyFilesBytes)(rootdir[, subpath, ...])  | Mapping interface to .py files' bytes                          |
| [`PyFilesReader`](#xdol.pystores.PyFilesReader)([store])                 | Mapping interface to .py files of a folder.                    |
| [`PyprojectReader`](#xdol.pystores.PyprojectReader)(src, \*[, max_levels]) | Mapping interface to pyproject.toml files in a directory tree. |
| [`SetupCfgReader`](#xdol.pystores.SetupCfgReader)(src, \*[, max_levels])  | Mapping interface to setup.cfg files in a directory tree.      |

### xdol.pystores.PkgFilesReader

alias of [`PyFilesReader`](#xdol.pystores.PyFilesReader)

### *class* xdol.pystores.PkgReader(rootdir, subpath='', pattern_for_field=None, max_levels=None, , include_hidden=False, assert_rootdir_existence=False, allow_escape=False)

Bases: `Store`

By store we mean key-value store. This could be files in a filesystem, objects in s3, or a database. Where and
how the content is stored should be specified, but StoreInterface offers a dict-like interface to this.

```default
__getitem__ calls: _id_of_key                                       _obj_of_data
__setitem__ calls: _id_of_key                   _data_of_obj
__delitem__ calls: _id_of_key
__iter__    calls:                  _key_of_id
```

```pycon
>>> # Default store: no key or value conversion #####################################
>>> from dol import Store
>>> s = Store()
>>> s['foo'] = 33
>>> s['bar'] = 65
>>> assert list(s.items()) == [('foo', 33), ('bar', 65)]
>>> assert list(s.store.items()) == [('foo', 33), ('bar', 65)]  # see that the store contains the same thing
>>>
>>> #################################################################################
>>> # Now let's make stores that have a key and value conversion layer ##############
>>> # input keys will be upper cased, and output keys lower cased ###################
>>> # input values (assumed int) will be converted to ascii string, and visa versa ##
>>> #################################################################################
>>>
>>> def test_store(s):
...     s['foo'] = 33  # write 33 to 'foo'
...     assert 'foo' in s  # __contains__ works
...     assert 'no_such_key' not in s  # __nin__ works
...     s['bar'] = 65  # write 65 to 'bar'
...     assert len(s) == 2  # there are indeed two elements
...     assert list(s) == ['foo', 'bar']  # these are the keys
...     assert list(s.keys()) == ['foo', 'bar']  # the keys() method works!
...     assert list(s.values()) == [33, 65]  # the values() method works!
...     assert list(s.items()) == [('foo', 33), ('bar', 65)]  # these are the items
...     assert list(s.store.items()) == [('FOO', '!'), ('BAR', 'A')]  # but note the internal representation
...     assert s.get('foo') == 33  # the get method works
...     assert s.get('no_such_key', 'something') == 'something'  # return a default value
...     del(s['foo'])  # you can delete an item given its key
...     assert len(s) == 1  # see, only one item left!
...     assert list(s.items()) == [('bar', 65)]  # here it is
>>>
>>> # We can introduce this conversion layer in several ways. Here's a few... ######################
>>> # by subclassing ###############################################################################
>>> class MyStore(Store):
...     def _id_of_key(self, k):
...         return k.upper()
...     def _key_of_id(self, _id):
...         return _id.lower()
...     def _data_of_obj(self, obj):
...         return chr(obj)
...     def _obj_of_data(self, data):
...         return ord(data)
>>> s = MyStore(store=dict())  # note that you don't need to specify dict(), since it's the default
>>> test_store(s)
>>>
>>> # by assigning functions to converters ##########################################################
>>> class MyStore(Store):
...     def __init__(self, store, _id_of_key, _key_of_id, _data_of_obj, _obj_of_data):
...         super().__init__(store)
...         self._id_of_key = _id_of_key
...         self._key_of_id = _key_of_id
...         self._data_of_obj = _data_of_obj
...         self._obj_of_data = _obj_of_data
...
>>> s = MyStore(dict(),
...             _id_of_key=lambda k: k.upper(),
...             _key_of_id=lambda _id: _id.lower(),
...             _data_of_obj=lambda obj: chr(obj),
...             _obj_of_data=lambda data: ord(data))
>>> test_store(s)
>>>
>>> # using a Mixin class #############################################################################
>>> class Mixin:
...     def _id_of_key(self, k):
...         return k.upper()
...     def _key_of_id(self, _id):
...         return _id.lower()
...     def _data_of_obj(self, obj):
...         return chr(obj)
...     def _obj_of_data(self, data):
...         return ord(data)
...
>>> class MyStore(Mixin, Store):  # note that the Mixin must come before Store in the mro
...     pass
...
>>> s = MyStore()  # no dict()? No, because default anyway
>>> test_store(s)
>>>
>>> # adding wrapper methods to an already made Store instance #########################################
>>> s = Store(dict())
>>> s._id_of_key=lambda k: k.upper()
>>> s._key_of_id=lambda _id: _id.lower()
>>> s._data_of_obj=lambda obj: chr(obj)
>>> s._obj_of_data=lambda data: ord(data)
>>> test_store(s)
```

Note on defining your own “Mapping Views”.

When you do a `.keys()`, a `.values()` or `.items()` you’re getting a `MappingView`
instance; an iterable and sized container that provides some methods to access
particular aspects of the wrapped mapping.

If you need to customize the behavior of these instances, you should avoid
overriding the `keys`, `values` or `items` methods directly, but instead
override the `KeysView`, `ValuesView` or `ItemsView` classes that they use.

For more, see: [https://github.com/i2mint/dol/wiki/Mapping-Views](https://github.com/i2mint/dol/wiki/Mapping-Views)

#### get(k) → D[k] if k in D, else d.  d defaults to None.

#### is_valid_key(k, \*args, \_\_name='is_valid_key', \*\*kwargs)

`is_valid_key` on the inner key – see `mk_relative_path_store`.

#### validate_key(k, \*args, \_\_name='validate_key', \*\*kwargs)

`validate_key` on the inner key – see `mk_relative_path_store`.

### *class* xdol.pystores.PyFilesBytes(rootdir, subpath='', pattern_for_field=None, max_levels=None, , include_hidden=False, assert_rootdir_existence=False, allow_escape=False)

Bases: `Store`

Mapping interface to .py files’ bytes

#### get(k) → D[k] if k in D, else d.  d defaults to None.

#### is_valid_key(k, \*args, \_\_name='is_valid_key', \*\*kwargs)

`is_valid_key` on the inner key – see `mk_relative_path_store`.

#### validate_key(k, \*args, \_\_name='validate_key', \*\*kwargs)

`validate_key` on the inner key – see `mk_relative_path_store`.

### *class* xdol.pystores.PyFilesReader(store=<class 'dict'>)

Bases: `PrefixRelativizationMixin`, `Store`

Mapping interface to .py files of a folder.
Keys are relative .py paths.
Values are the string contents of the .py file.

Important Note: If the byte contents of the .py file can’t be decoded (with a simple bytes.decode()),
an empty string will be returned as it’s value (i.e. contents).

```pycon
>>> import asyncio
>>> s = PyFilesReader(asyncio)
>>> assert len(s) > 10  # to test length (currently asyncio has 29 modules
>>> 'locks.py' in s
True
```

But you can also specify an \_\_init_\_.py filepath or the directory containing it.

```pycon
>>> import os
>>> init_filepath = asyncio.__file__
>>> dirpath_to_asyncio_modules = os.path.dirname(init_filepath)
>>> ss = PyFilesReader(init_filepath)
>>> sss = PyFilesReader(dirpath_to_asyncio_modules)
>>> assert list(s) == list(ss) == list(sss)
```

#### is_valid_key(k, \*args, \_\_name='is_valid_key', \*\*kwargs)

`is_valid_key` on the inner key – see `mk_relative_path_store`.

#### validate_key(k, \*args, \_\_name='validate_key', \*\*kwargs)

`validate_key` on the inner key – see `mk_relative_path_store`.

### *class* xdol.pystores.PyprojectReader(src, , max_levels=None)

Bases: `Store`

Mapping interface to pyproject.toml files in a directory tree.
Keys are relative paths to pyproject.toml files.
Values are the string contents of the pyproject.toml files.

```pycon
>>> import tempfile, os
>>> with tempfile.TemporaryDirectory() as tmpdir:
...     # Create a pyproject.toml file
...     proj1_dir = os.path.join(tmpdir, 'proj1')
...     os.makedirs(proj1_dir)
...     toml_path = os.path.join(proj1_dir, 'pyproject.toml')
...     with open(toml_path, 'w') as f:
...         _ = f.write('[project]\ndependencies = ["requests>=2.0"]')
...
...     reader = PyprojectReader(tmpdir)
...     assert len(reader) == 1
...     key = os.path.join('proj1', 'pyproject.toml')  # os-specific separator
...     assert key in reader
...     assert 'requests' in reader[key]
```

#### get(k) → D[k] if k in D, else d.  d defaults to None.

#### is_valid_key(k, \*args, \_\_name='is_valid_key', \*\*kwargs)

`is_valid_key` on the inner key – see `mk_relative_path_store`.

#### validate_key(k, \*args, \_\_name='validate_key', \*\*kwargs)

`validate_key` on the inner key – see `mk_relative_path_store`.

### *class* xdol.pystores.SetupCfgReader(src, , max_levels=None)

Bases: `Store`

Mapping interface to setup.cfg files in a directory tree.
Keys are relative paths to setup.cfg files.
Values are the string contents of the setup.cfg files.

```pycon
>>> import tempfile, os
>>> with tempfile.TemporaryDirectory() as tmpdir:
...     # Create some setup.cfg files
...     proj1_dir = os.path.join(tmpdir, 'proj1')
...     os.makedirs(proj1_dir)
...     cfg_path = os.path.join(proj1_dir, 'setup.cfg')
...     with open(cfg_path, 'w') as f:
...         _ = f.write('[options]\ninstall_requires = requests')
...
...     reader = SetupCfgReader(tmpdir)
...     assert len(reader) == 1
...     key = os.path.join('proj1', 'setup.cfg')  # os-specific separator
...     assert key in reader
...     assert 'requests' in reader[key]
```

#### get(k) → D[k] if k in D, else d.  d defaults to None.

#### is_valid_key(k, \*args, \_\_name='is_valid_key', \*\*kwargs)

`is_valid_key` on the inner key – see `mk_relative_path_store`.

#### validate_key(k, \*args, \_\_name='validate_key', \*\*kwargs)

`validate_key` on the inner key – see `mk_relative_path_store`.
