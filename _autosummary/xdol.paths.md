# xdol.paths

Tools for manipulating paths.

A path is a sequence of objects that represent a reference or location.
Often paths are strings that use a special character (e.g. ‘/’) to distinguish the
separate objects in the sequence.

### Functions

| [`extract_path_segments`](#xdol.paths.extract_path_segments)(path, \*[, sep, depth, ...])   | Extracts the first `depth` segments of a path, delimited by `sep`.   |
|-------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|

### xdol.paths.extract_path_segments(path, , sep='/', depth=1, trailing_sep=True)

Extracts the first `depth` segments of a path, delimited by `sep`.

* **Parameters:**
  * **path** – The path to extract the segments from.
  * **sep** – The delimiter to use to separate the segments.
  * **depth** – The number of segments to extract.
  * **trailing_sep** – Whether to add a trailing delimiter to the result.
* **Returns:**
  The first `depth` segments of the path, delimited by `sep`.

By default, the function will use depth=1 and the system’s path separator as `sep`.

```pycon
>>> extract_path_segments("snap/crackle/pop/cereal", sep='/')
'snap/'
```

You can specify a different `depth`.

```pycon
>>> extract_path_segments("snap/crackle/pop/cereal", depth=2, sep='/')
'snap/crackle/'
```

Note that if the path starts with the `sep` delimiter, the result will be different.

```pycon
>>> extract_path_segments("/snap/crackle/pop/cereal", depth=2, sep='/')
'/snap/'
```

If `path` is not a string, the function will assume it’s an iterable of strings,
and return an iterable of the results.

```pycon
>>> list(extract_path_segments(
...     ['a.file', 'a/folder', 'a/folder_with/subfolders/', 'another/folder'],
...     depth=2, sep='/'
... ))
['a.file', 'a/folder', 'a/folder_with/', 'another/folder']
```

You can of course specify a different delimiter.

```pycon
>>> extract_path_segments("snap.crackle.pop.cereal", sep='.', depth=2)
'snap.crackle.'
```

That last dot doesn’t make sense here? Tell the function to not add the separator
at the end of the result.

```pycon
>>> extract_path_segments(
...     "snap.crackle.pop.cereal", sep='.', depth=2, trailing_sep=False
... )
'snap.crackle'
```
