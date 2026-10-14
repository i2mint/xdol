# xdol.x_codecs

Extra codecs: object <-> bytes pairs, and stores that apply them.

Besides `resolve_values_to_bytes()`, this module holds the value-encoding half of the
persistent memoizer ([`xdol.memoize`](xdol.memoize.html.md#module-xdol.memoize)):

- [`Codec`](#xdol.x_codecs.Codec) – an `encode`/`decode` pair, with ready-made [`JSON_CODEC`](#xdol.x_codecs.JSON_CODEC)
  (human-readable, diff-friendly), [`TEXT_CODEC`](#xdol.x_codecs.TEXT_CODEC) and [`BYTES_CODEC`](#xdol.x_codecs.BYTES_CODEC)
  (pass-through, for images/audio), looked up by name through [`get_codec()`](#xdol.x_codecs.get_codec);
- [`with_codec()`](#xdol.x_codecs.with_codec) – apply a codec to the values of a bytes store;
- [`paired_store()`](#xdol.x_codecs.paired_store) – for a result that mixes metadata and a binary asset, a store
  whose value is `(metadata, asset_bytes)`, persisted as `<key>.json` + `<key>.bin`.

```pycon
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
```

### Module Attributes

| [`JSON_CODEC`](#xdol.x_codecs.JSON_CODEC)   | Text / JSON results, stored human-readably so a committed cache is reviewable.   |
|---------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`TEXT_CODEC`](#xdol.x_codecs.TEXT_CODEC)   | `str` results, stored as UTF-8.                                                  |
| [`BYTES_CODEC`](#xdol.x_codecs.BYTES_CODEC)  | Pass-through, for binary assets (image / audio bytes).                           |
| [`CODECS`](#xdol.x_codecs.CODECS)       | `CODECS["pickle"] = Codec(pickle.dumps, pickle.loads)`.                          |

### Functions

| [`get_codec`](#xdol.x_codecs.get_codec)(codec)                  | Resolve a [`Codec`](#xdol.x_codecs.Codec), a name in [`CODECS`](#xdol.x_codecs.CODECS), or an `(encode, decode)` pair.   |
|------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`json_codec`](#xdol.x_codecs.json_codec)(\*[, indent, default]) | A JSON [`Codec`](#xdol.x_codecs.Codec): keys sorted, non-ASCII kept, so stored files diff well.                                                |
| [`paired_store`](#xdol.x_codecs.paired_store)(store, \*\*kwargs)   | See [`PairedStore`](#xdol.x_codecs.PairedStore).                                                                                                     |
| [`with_codec`](#xdol.x_codecs.with_codec)(store[, codec])        | Wrap a bytes `store` so its values are objects, encoded/decoded by `codec`.                                                                                           |

### Classes

| [`Codec`](#xdol.x_codecs.Codec)(encode, decode)                     | An object <-> bytes pair, for the values of a store.                              |
|--------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`PairedStore`](#xdol.x_codecs.PairedStore)(store, \*[, meta_codec, ...]) | Values are `(metadata, asset_bytes)` pairs, kept as two entries in a bytes store. |

### Exceptions

| [`CodecDecodeError`](#xdol.x_codecs.CodecDecodeError)   | A stored value could not be decoded (corrupt or truncated entry, wrong codec).   |
|---------------------------------------------------------------------|----------------------------------------------------------------------------------|

### xdol.x_codecs.BYTES_CODEC *= (<function \_as_bytes>, <function <lambda>>)*

Pass-through, for binary assets (image / audio bytes).

### xdol.x_codecs.CODECS *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [Codec](#xdol.x_codecs.Codec)]* *= {'bytes': (<function \_as_bytes>, <function <lambda>>), 'json': (<function json_codec.<locals>.encode>, <function json_codec.<locals>.<lambda>>), 'text': (<function <lambda>>, <function <lambda>>)}*

`CODECS["pickle"] = Codec(pickle.dumps, pickle.loads)`.

* **Type:**
  Named codecs. Register your own

### *class* xdol.x_codecs.Codec(encode, decode)

Bases: [`NamedTuple`](https://docs.python.org/3/library/typing.html#typing.NamedTuple)

An object <-> bytes pair, for the values of a store.

#### decode *: [Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[bytes](https://docs.python.org/3/builtins/stdtypes.html#bytes)], [Any](https://docs.python.org/3/library/typing.html#typing.Any)]*

Alias for field number 1

#### encode *: [Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[Any](https://docs.python.org/3/library/typing.html#typing.Any)], [bytes](https://docs.python.org/3/builtins/stdtypes.html#bytes)]*

Alias for field number 0

### *exception* xdol.x_codecs.CodecDecodeError

Bases: [`ValueError`](https://docs.python.org/3/builtins/exceptions.html#ValueError)

A stored value could not be decoded (corrupt or truncated entry, wrong codec).

Raised by [`with_codec()`](#xdol.x_codecs.with_codec) and [`PairedStore`](#xdol.x_codecs.PairedStore) in place of whatever the
decoder raised (chained as `__cause__`), so callers can tell “this entry is
undecodable” apart from store, network or configuration failures.

### xdol.x_codecs.JSON_CODEC *= (<function json_codec.<locals>.encode>, <function json_codec.<locals>.<lambda>>)*

Text / JSON results, stored human-readably so a committed cache is reviewable.

### *class* xdol.x_codecs.PairedStore(store, , meta_codec='json', meta_suffix='.json', asset_suffix='.bin')

Bases: [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)

Values are `(metadata, asset_bytes)` pairs, kept as two entries in a bytes store.

`store[k] = (meta, blob)` writes `k + asset_suffix` (`blob` verbatim) and
`k + meta_suffix` (`{"asset_sha256": ..., "meta": meta}` through `meta_codec`).
The metadata entry is written *last* and is what makes a key exist, so a crash
mid-write leaves no half-entry that reads back as valid; `in`/iteration/`len`
only see keys whose both halves are present. The digest guards against two writers
of the same key interleaving (A’s metadata with B’s asset): such a pair raises
[`CodecDecodeError`](#xdol.x_codecs.CodecDecodeError) on read instead of returning the chimera. Entries in the
older bare-metadata layout are still read (unverified).

### xdol.x_codecs.TEXT_CODEC *= (<function <lambda>>, <function <lambda>>)*

`str` results, stored as UTF-8.

### xdol.x_codecs.get_codec(codec)

Resolve a [`Codec`](#xdol.x_codecs.Codec), a name in [`CODECS`](#xdol.x_codecs.CODECS), or an `(encode, decode)` pair.

* **Return type:**
  [`Codec`](#xdol.x_codecs.Codec)

```pycon
>>> get_codec("json") is JSON_CODEC
True
>>> get_codec((str.encode, bytes.decode)).decode(b"x")
'x'
```

### xdol.x_codecs.json_codec(, indent=2, default=None)

A JSON [`Codec`](#xdol.x_codecs.Codec): keys sorted, non-ASCII kept, so stored files diff well.

Strict by default: a value JSON cannot represent raises `TypeError` rather than
being silently stringified. Pass `default=` (as in [`json.dumps()`](https://docs.python.org/3/library/json.html#json.dumps)) to opt in.

* **Return type:**
  [`Codec`](#xdol.x_codecs.Codec)

```pycon
>>> c = json_codec(indent=None)
>>> c.decode(c.encode({"b": "é", "a": 1}))
{'a': 1, 'b': 'é'}
>>> c.encode({"b": 1, "a": 2})
b'{"a": 2, "b": 1}'
```

### xdol.x_codecs.paired_store(store, \*\*kwargs)

See [`PairedStore`](#xdol.x_codecs.PairedStore). `store` is a bytes `MutableMapping` (files, S3, …).

* **Return type:**
  [`PairedStore`](#xdol.x_codecs.PairedStore)

### xdol.x_codecs.with_codec(store, codec='json')

Wrap a bytes `store` so its values are objects, encoded/decoded by `codec`.

A value that fails to decode raises [`CodecDecodeError`](#xdol.x_codecs.CodecDecodeError).

* **Return type:**
  [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)
