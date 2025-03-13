"""Tools for manipulating paths.

A path is a sequence of objects that represent a reference or location.
Often paths are strings that use a special character (e.g. '/') to distinguish the
separate objects in the sequence.

"""

import os


def extract_path_segments(path, *, sep=os.path.sep, depth=1, trailing_sep=True):
    """Extracts the first `depth` segments of a path, delimited by `sep`.

    :param path: The path to extract the segments from.
    :param sep: The delimiter to use to separate the segments.
    :param depth: The number of segments to extract.
    :param trailing_sep: Whether to add a trailing delimiter to the result.
    :return: The first `depth` segments of the path, delimited by `sep`.

    By default, the function will use depth=1 and the system's path separator as `sep`.

    >>> extract_path_segments("snap/crackle/pop/cereal", sep='/')
    'snap/'

    You can specify a different `depth`.

    >>> extract_path_segments("snap/crackle/pop/cereal", depth=2, sep='/')
    'snap/crackle/'

    Note that if the path starts with the `sep` delimiter, the result will be different.

    >>> extract_path_segments("/snap/crackle/pop/cereal", depth=2, sep='/')
    '/snap/'

    If `path` is not a string, the function will assume it's an iterable of strings,
    and return an iterable of the results.

    >>> list(extract_path_segments(
    ...     ['a.file', 'a/folder', 'a/folder_with/subfolders/', 'another/folder'],
    ...     depth=2, sep='/'
    ... ))
    ['a.file', 'a/folder', 'a/folder_with/', 'another/folder']

    You can of course specify a different delimiter.

    >>> extract_path_segments("snap.crackle.pop.cereal", sep='.', depth=2)
    'snap.crackle.'

    That last dot doesn't make sense here? Tell the function to not add the separator
    at the end of the result.

    >>> extract_path_segments(
    ...     "snap.crackle.pop.cereal", sep='.', depth=2, trailing_sep=False
    ... )
    'snap.crackle'

    """
    if not isinstance(path, str):
        from functools import partial

        paths = path  # path is actually an iterable of paths
        return map(partial(extract_path_segments, sep=sep, depth=depth), paths)
    else:
        if depth < 1:
            raise ValueError("depth must be a positive integer")

        segments = path.split(sep)

        if depth >= len(segments):
            # If the depth specified exceeds the actual number of segments in the path,
            # return the entire path as is
            return path

        # Join the segments up to the specified depth with the delimiter
        result = sep.join(segments[:depth])

        # Append a trailing delimiter to indicate the last segment
        if trailing_sep:
            result += sep

        return result
