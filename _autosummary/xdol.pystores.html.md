# xdol.pystores

Stores for python objects

### Functions

| `bytes_decoder`(x)   |    |
|----------------------|----|

### Classes

| [`PkgFilesReader`](#xdol.pystores.PkgFilesReader)                          |                                                                                                                                            |
|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| [`PkgReader`](#xdol.pystores.PkgReader)(rootdir[, subpath, ...])      | Mapping from the sub-package directories of a package to [`PyFilesReader`](#xdol.pystores.PyFilesReader) of their contents. |
| [`PyFilesBytes`](#xdol.pystores.PyFilesBytes)(\*args[, read_retry, ...]) | Mapping interface to .py files' bytes                                                                                                      |
| [`PyFilesReader`](#xdol.pystores.PyFilesReader)([store])                  | Mapping interface to .py files of a folder.                                                                                                |
| [`PyprojectReader`](#xdol.pystores.PyprojectReader)(src, \*[, max_levels])  | Mapping interface to pyproject.toml files in a directory tree.                                                                             |
| [`SetupCfgReader`](#xdol.pystores.SetupCfgReader)(src, \*[, max_levels])   | Mapping interface to setup.cfg files in a directory tree.                                                                                  |

### xdol.pystores.PkgFilesReader

alias of [`PyFilesReader`](#xdol.pystores.PyFilesReader)

### *class* xdol.pystores.PkgReader(rootdir, subpath='', pattern_for_field=None, max_levels=None, , include_hidden=False, assert_rootdir_existence=False, allow_escape=False)

Bases: `Store`

Mapping from the sub-package directories of a package to [`PyFilesReader`](#xdol.pystores.PyFilesReader) of their contents.

#### get(k) → D[k] if k in D, else d.  d defaults to None.

#### is_valid_key(k, \*args, \_\_name='is_valid_key', \*\*kwargs)

`is_valid_key` on the inner key – see `mk_relative_path_store`.

#### validate_key(k, \*args, \_\_name='validate_key', \*\*kwargs)

`validate_key` on the inner key – see `mk_relative_path_store`.

### *class* xdol.pystores.PyFilesBytes(\*args, read_retry=None, strict_key_errors=False, \*\*kwargs)

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
