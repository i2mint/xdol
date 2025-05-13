"""Extra codecs"""

from typing import Iterable
import json
from dol import wrap_kvs


# TODO: Make it into a plugin (enable encoding registration)
#   See: [dol: Standard lib support for postget and preset](https://github.com/i2mint/dol/discussions/46#discussioncomment-13131631)
def _resolve_values_to_bytes(v, *, encoding="utf-8"):
    """
    >>> wrapped_dict = resolve_values_to_bytes(dict)
    >>> d = wrapped_dict()
    >>> d['a'] = 'hello'
    >>> d['b'] = 123
    >>> d['c'] = [1, 2, 3]
    >>> d['d'] = {'a': 1, 'b': 2}
    >>> d['e'] = None
    >>> d
    {'a': b'hello', 'b': b'123', 'c': b'[1, 2, 3]', 'd': b'{"a": 1, "b": 2}', 'e': b'null'}
    """
    if isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return v.encode(encoding)
    elif isinstance(v, (list, tuple, dict, int, float, bool)) or v is None:
        return json.dumps(v).encode(encoding)
    elif isinstance(v, Iterable):
        return json.dumps(list(v)).encode(encoding)
    else:
        return v


resolve_values_to_bytes = wrap_kvs(value_encoder=_resolve_values_to_bytes)
resolve_values_to_bytes.__doc__ = (
    """
    Store wrapper that encodes values (bytes, str, iterables) to bytes.
    It uses json.dumps for lists, tuples, dicts, ints, floats, and bools.
    For other types, it uses the default encoding.

"""
    + _resolve_values_to_bytes.__doc__
)
