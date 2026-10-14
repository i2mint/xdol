"""Extra codecs: object <-> bytes pairs, and stores that apply them.

Besides :func:`resolve_values_to_bytes`, this module holds the value-encoding half of the
persistent memoizer (:mod:`xdol.memoize`):

- :class:`Codec` -- an ``encode``/``decode`` pair, with ready-made :data:`JSON_CODEC`
  (human-readable, diff-friendly), :data:`TEXT_CODEC` and :data:`BYTES_CODEC`
  (pass-through, for images/audio), looked up by name through :func:`get_codec`;
- :func:`with_codec` -- apply a codec to the values of a bytes store;
- :func:`paired_store` -- for a result that mixes metadata and a binary asset, a store
  whose value is ``(metadata, asset_bytes)``, persisted as ``<key>.json`` + ``<key>.bin``.

>>> store = with_codec({}, "json")
>>> store["k"] = {"b": 1, "a": [1, 2]}
>>> store["k"]
{'a': [1, 2], 'b': 1}
>>> raw = {}
>>> pairs = paired_store(raw)
>>> pairs["img1"] = ({"prompt": "a cat"}, b"PNGDATA")
>>> sorted(raw)
['img1.bin', 'img1.json']
>>> pairs["img1"]
({'prompt': 'a cat'}, b'PNGDATA')
"""

from collections.abc import Callable, Iterable, Iterator, MutableMapping
import json
from typing import Any, NamedTuple
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


# ---------------------------------------------------------------------------
# Codecs: object <-> bytes
# ---------------------------------------------------------------------------


class Codec(NamedTuple):
    """An object <-> bytes pair, for the values of a store."""

    encode: Callable[[Any], bytes]
    decode: Callable[[bytes], Any]


def json_codec(*, indent: int | None = 2, default: Callable | None = None) -> Codec:
    """A JSON :class:`Codec`: keys sorted, non-ASCII kept, so stored files diff well.

    Strict by default: a value JSON cannot represent raises ``TypeError`` rather than
    being silently stringified. Pass ``default=`` (as in :func:`json.dumps`) to opt in.

    >>> c = json_codec(indent=None)
    >>> c.decode(c.encode({"b": "é", "a": 1}))
    {'a': 1, 'b': 'é'}
    >>> c.encode({"b": 1, "a": 2})
    b'{"a": 2, "b": 1}'
    """

    def encode(obj):
        kw = dict(indent=indent, ensure_ascii=False, default=default)
        try:
            text = json.dumps(obj, sort_keys=True, **kw)
        except TypeError:  # unsortable (mixed-type) keys, or an unserializable value
            text = json.dumps(obj, **kw)
        return text.encode("utf-8")

    return Codec(encode, lambda data: json.loads(data))


JSON_CODEC = json_codec()
"""Text / JSON results, stored human-readably so a committed cache is reviewable."""

TEXT_CODEC = Codec(lambda s: s.encode("utf-8"), lambda b: b.decode("utf-8"))
"""``str`` results, stored as UTF-8."""

def _as_bytes(obj) -> bytes:
    if not isinstance(obj, (bytes, bytearray, memoryview)):
        raise TypeError(f"Expected bytes-like, got {type(obj).__name__}")
    return bytes(obj)


BYTES_CODEC = Codec(_as_bytes, lambda data: data)
"""Pass-through, for binary assets (image / audio bytes)."""

CODECS: dict[str, Codec] = {
    "json": JSON_CODEC,
    "text": TEXT_CODEC,
    "bytes": BYTES_CODEC,
}
"""Named codecs. Register your own: ``CODECS["pickle"] = Codec(pickle.dumps, pickle.loads)``."""


def get_codec(codec: "Codec | str | tuple[Callable, Callable]") -> Codec:
    """Resolve a :class:`Codec`, a name in :data:`CODECS`, or an ``(encode, decode)`` pair.

    >>> get_codec("json") is JSON_CODEC
    True
    >>> get_codec((str.encode, bytes.decode)).decode(b"x")
    'x'
    """
    if isinstance(codec, str):
        try:
            return CODECS[codec]
        except KeyError:
            raise KeyError(f"Unknown codec {codec!r}; known: {sorted(CODECS)}") from None
    return codec if isinstance(codec, Codec) else Codec(*codec)


def with_codec(store: MutableMapping, codec="json") -> MutableMapping:
    """Wrap a bytes ``store`` so its values are objects, encoded/decoded by ``codec``."""
    codec = get_codec(codec)
    return wrap_kvs(store, value_encoder=codec.encode, value_decoder=codec.decode)


# ---------------------------------------------------------------------------
# Composite results: metadata + binary asset
# ---------------------------------------------------------------------------


class PairedStore(MutableMapping):
    """Values are ``(metadata, asset_bytes)`` pairs, kept as two entries in a bytes store.

    ``store[k] = (meta, blob)`` writes ``k + meta_suffix`` (``meta`` through
    ``meta_codec``) and ``k + asset_suffix`` (``blob`` verbatim). The metadata entry is
    written *last* and is what makes a key exist, so a crash mid-write leaves no
    half-entry that reads back as valid; ``in``/iteration/``len`` only see keys whose
    both halves are present.
    """

    def __init__(
        self,
        store: MutableMapping,
        *,
        meta_codec="json",
        meta_suffix: str = ".json",
        asset_suffix: str = ".bin",
    ):
        if (
            not meta_suffix
            or not asset_suffix
            or meta_suffix.endswith(asset_suffix)
            or asset_suffix.endswith(meta_suffix)
        ):
            raise ValueError(
                "meta_suffix and asset_suffix must be non-empty and neither may end "
                "with the other (the entries of different keys would overlap)"
            )
        self.store = store
        self.meta_codec = get_codec(meta_codec)
        self.meta_suffix = meta_suffix
        self.asset_suffix = asset_suffix

    def __contains__(self, key) -> bool:
        return (
            isinstance(key, str)
            and (key + self.meta_suffix) in self.store
            and (key + self.asset_suffix) in self.store
        )

    def __getitem__(self, key):
        if key not in self:
            raise KeyError(key)
        meta = self.meta_codec.decode(self.store[key + self.meta_suffix])
        return meta, self.store[key + self.asset_suffix]

    def __setitem__(self, key, value):
        try:
            meta, asset = value
            asset = _as_bytes(asset)
        except (TypeError, ValueError):
            raise TypeError(
                "A paired store holds (metadata, asset_bytes) pairs; "
                f"got {type(value).__name__}"
            ) from None
        encoded_meta = self.meta_codec.encode(meta)  # fail before writing anything
        if (meta_key := key + self.meta_suffix) in self.store:
            del self.store[meta_key]  # un-commit first: a failed write below must not
            # leave the old metadata paired with the new asset
        self.store[key + self.asset_suffix] = asset
        self.store[key + self.meta_suffix] = encoded_meta

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        del self.store[key + self.meta_suffix]  # un-commit first
        del self.store[key + self.asset_suffix]

    def __iter__(self) -> Iterator[str]:
        n = len(self.meta_suffix)
        for k in list(self.store):
            if k.endswith(self.meta_suffix) and (k[:-n] + self.asset_suffix) in self.store:
                yield k[:-n]

    def __len__(self) -> int:
        return sum(1 for _ in self)


def paired_store(store: MutableMapping, **kwargs) -> PairedStore:
    """See :class:`PairedStore`. ``store`` is a bytes ``MutableMapping`` (files, S3, ...)."""
    return PairedStore(store, **kwargs)
