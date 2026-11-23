"""Tools that left ``dol``'s top-level namespace for xdol (dol issue #145).

``dol`` keeps its core small. These names were exported at ``dol``'s top level but used
nowhere inside dol, so their public home is now xdol: import them from here (or from
``xdol`` directly). ``dol.<name>`` still works for one release, with a
``DeprecationWarning`` pointing here.

``kvhead`` and ``ihead`` are defined here. The others are, for now, re-exported from the
dol submodule that still defines them (so there is one definition of each); when dol
removes them, their definitions move into this module.

>>> from xdol import kvhead, ihead, igroupby
>>> kvhead({'a': 1, 'b': 2})
('a', 1)
>>> ihead('abc', 2)
['a', 'b']
>>> dict(igroupby(['apple', 'avocado', 'banana'], key=lambda w: w[0]))
{'a': ['apple', 'avocado'], 'b': ['banana']}
"""

from itertools import islice
from dol.util import AttributeMutableMapping, igroupby, regroupby
from dol.explicit import ExplicitKeyMap
from dol.sources import SequenceKvReader
from dol.naming import mk_store_from_path_format_store_cls
from dol.trans import redirect_getattr_to_getitem
from dol.dig import trace_getitem
from dol.zipfiledol import to_zip_file

__all__ = [
    "AttributeMutableMapping",
    "ExplicitKeyMap",
    "SequenceKvReader",
    "igroupby",
    "regroupby",
    "ihead",
    "kvhead",
    "mk_store_from_path_format_store_cls",
    "redirect_getattr_to_getitem",
    "trace_getitem",
    "to_zip_file",
]


def kvhead(store, n=1):
    """Get the first item of a kv store, or a list of the first n items

    >>> kvhead({'a': 1, 'b': 2}, 5)
    [('a', 1), ('b', 2)]
    >>> kvhead({}) is None
    True
    """
    if n == 1:
        for k in store:
            return k, store[k]
    else:
        return [(k, store[k]) for k in islice(store, n)]


def ihead(store, n=1):
    """Get the first item of an iterable, or a list of the first n items

    >>> ihead(iter('abc'))
    'a'
    >>> ihead(iter('')) is None
    True
    """
    if n == 1:
        for item in iter(store):
            return item
    else:
        return list(islice(store, n))
