"""Utility functions for xdol."""

import os
import inspect


# Pattern: meshed
def resolve_module_filepath(
    module_spec, assert_output_is_existing_filepath=True
) -> str:
    if inspect.ismodule(module_spec):
        module_spec = inspect.getsourcefile(module_spec)
    elif not isinstance(module_spec, str):
        module_spec = inspect.getfile(module_spec)
    if module_spec.endswith("c"):
        module_spec = module_spec[:-1]  # remove the 'c' of '.pyc'
    if os.path.isdir(module_spec):
        module_dir = module_spec
        module_spec = os.path.join(module_dir, "__init__.py")
        assert os.path.isfile(module_spec), (
            f"You specified the module as a directory {module_dir}, "
            f"but this directory wasn't a package (it didn't have an __init__.py file)"
        )
    if assert_output_is_existing_filepath:
        assert os.path.isfile(module_spec), "module_spec should be a file at this point"
    return module_spec


# Pattern: meshed
def resolve_to_folder(obj, assert_output_is_existing_folder=True):
    if inspect.ismodule(obj):
        obj = inspect.getsourcefile(obj)
    elif not isinstance(obj, str):
        obj = inspect.getfile(obj)

    if not os.path.isdir(obj):
        if obj.endswith("c"):
            obj = obj[:-1]  # remove the 'c' of '.pyc'
        if obj.endswith("__init__.py"):
            obj = os.path.dirname(obj)
    if assert_output_is_existing_folder:
        assert os.path.isdir(obj), "obj should be a folder at this point"
    return obj


# ------------------------------------------------------------------------------
# Object saving utility
import pickle
import tempfile
from pathlib import Path
from typing import Callable, Any
from datetime import datetime


def save_obj(
    obj: Any,
    *,
    encode: Callable[[Any], bytes] = pickle.dumps,
    key: Callable[[Any, bytes], str] | None = None,
    save_under_key: Callable[[str, bytes], None] | Any = None,
    return_func: Callable[[Any, bytes, str], Any] | None = None,
) -> Any:
    """
    Serialize and save a Python object with customizable encoding and storage.

    By default, pickles the object and saves it to a temp directory with a
    timestamp-based filename, returning the full filepath. All behaviors can
    be customized for different serialization, naming, storage, or return formats.

    Args:
        obj: The object to save
        encode: Function to encode obj to bytes (default: pickle.dumps)
        key: Function taking (obj, encoded_obj) and returning a key string.
             If None, generates a hex timestamp-based key.
        save_under_key: Function taking (key, encoded_obj), or object with __setitem__.
                       If None, saves to 'obj_dumps' subdirectory in temp directory.
        return_func: Function taking (obj, encoded_obj, key) and returning the result.
                    If None, returns filepath (default storage) or key (custom storage).

    Returns:
        By default, the filepath string where the object was saved

    >>> result = save_obj({'test': 123})

    By default, saves the pickle serialization of the object to a temp file and
    returns the filepath. The filename that is created is a hex-encoded timestamp
    with a .pkl extension.

    >>> result  # doctest: +SKIP
    '/var/folders/.../1190a0e0ec5cde143.pkl'
    >>> import os
    >>> os.path.exists(result)
    True
    >>> result.endswith('.pkl')
    True

    """
    encoded_obj = encode(obj)

    # Generate key
    key_func = key if key is not None else _default_key_generator
    key_str = key_func(obj, encoded_obj)

    # Determine save function and track if using default
    using_default_save = save_under_key is None

    if save_under_key is None:
        save_func = _default_save_to_temp
    elif callable(save_under_key):
        save_func = save_under_key
    elif hasattr(save_under_key, "__setitem__"):

        def save_func(k, v):
            save_under_key[k] = v

    else:
        raise TypeError(
            "save_under_key must be None, a callable, or have a __setitem__ method"
        )

    # Save the object
    save_func(key_str, encoded_obj)

    # Determine return value
    if return_func is not None:
        return return_func(obj, encoded_obj, key_str)
    elif using_default_save:
        return _get_default_filepath(key_str)
    else:
        return key_str


def _default_key_generator(obj: Any, encoded_obj: bytes) -> str:
    """
    Generate a hex-encoded timestamp-based key with .pkl extension.

    >>> key = _default_key_generator(None, b'')
    >>> key.endswith('.pkl')
    True
    >>> len(key) > 4  # Has timestamp + extension
    True
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return hex(int(timestamp))[2:] + ".pkl"


def _default_save_to_temp(key: str, encoded_obj: bytes) -> None:
    """
    Save encoded object to temp directory under 'obj_dumps' subdirectory.

    Creates the directory if it doesn't exist.
    """
    temp_dir = Path(tempfile.gettempdir()) / "obj_dumps"
    temp_dir.mkdir(exist_ok=True)
    filepath = temp_dir / key
    filepath.write_bytes(encoded_obj)


def _get_default_filepath(key: str) -> str:
    """
    Get the default filepath for a given key.

    >>> path = _get_default_filepath('test.pkl')
    >>> 'obj_dumps' in path
    True
    >>> path.endswith('test.pkl')
    True
    """
    temp_dir = Path(tempfile.gettempdir()) / "obj_dumps"
    return str(temp_dir / key)
