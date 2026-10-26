# xdol.util

Utility functions for xdol.

### Functions

| `resolve_module_filepath`(module_spec[, ...])                                          |                                                                            |
|----------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| `resolve_to_folder`(obj[, ...])                                                        |                                                                            |
| [`save_obj`](#xdol.util.save_obj)(obj, \*[, encode, key, ...]) | Serialize and save a Python object with customizable encoding and storage. |

### xdol.util.save_obj(obj, \*, encode=<built-in function dumps>, key=None, save_under_key=None, return_func=None)

Serialize and save a Python object with customizable encoding and storage.

By default, pickles the object and saves it to a temp directory with a
timestamp-based filename, returning the full filepath. All behaviors can
be customized for different serialization, naming, storage, or return formats.

* **Parameters:**
  * **obj** ([`Any`](https://docs.python.org/3/library/typing.html#typing.Any)) – The object to save
  * **encode** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)], [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes)]) – Function to encode obj to bytes (default: pickle.dumps)
  * **key** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any), [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes)], [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Function taking (obj, encoded_obj) and returning a key string.
    If None, generates a hex timestamp-based key.
  * **save_under_key** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes)], [`None`](https://docs.python.org/3/builtins/constants.html#None)] | [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)) – Function taking (key, encoded_obj), or object with \_\_setitem_\_.
    If None, saves to ‘obj_dumps’ subdirectory in temp directory.
  * **return_func** ([`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any), [`bytes`](https://docs.python.org/3/builtins/stdtypes.html#bytes), [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)], [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Function taking (obj, encoded_obj, key) and returning the result.
    If None, returns filepath (default storage) or key (custom storage).
* **Return type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)
* **Returns:**
  By default, the filepath string where the object was saved

```pycon
>>> result = save_obj({'test': 123})
```

By default, saves the pickle serialization of the object to a temp file and
returns the filepath. The filename that is created is a hex-encoded timestamp
with a .pkl extension.

```pycon
>>> result
'/var/folders/.../1190a0e0ec5cde143.pkl'
>>> import os
>>> os.path.exists(result)
True
>>> result.endswith('.pkl')
True
```
