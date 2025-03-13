"""Stores for python objects"""

import site
import os
from dol import wrap_kvs, filt_iter, KvReader, cached_keys, Pipe, Files
from dol.filesys import mk_relative_path_store, DirCollection, FileBytesReader
from xdol.util import resolve_to_folder


@filt_iter(filt=lambda k: k.endswith(".py") and "__pycache__" not in k)
@mk_relative_path_store(prefix_attr="rootdir")
class PyFilesBytes(FileBytesReader):
    """Mapping interface to .py files' bytes"""


# Note: One could use a more robust bytes decoder (like tec.util.decode_or_default)
bytes_decoder = lambda x: x.decode()

py_files_wrap = Pipe(
    wrap_kvs(obj_of_data=bytes_decoder),
    filt_iter(filt=lambda k: k.endswith(".py") and "__pycache__" not in k),
    mk_relative_path_store(prefix_attr="rootdir"),
)


# TODO: Extend PyFilesReader to take more kinds of src arguments.
#   for example: single .py filepaths or iterables thereof (use cached_keys for that)
# @wrap_kvs(obj_of_data=bytes_decoder)
# @filt_iter(filt=lambda k: k.endswith('.py') and '__pycache__' not in k)
# @mk_relative_path_store(prefix_attr='rootdir')
@py_files_wrap
class PyFilesReader(FileBytesReader, KvReader):
    """Mapping interface to .py files of a folder.
    Keys are relative .py paths.
    Values are the string contents of the .py file.

    Important Note: If the byte contents of the .py file can't be decoded (with a simple bytes.decode()),
    an empty string will be returned as it's value (i.e. contents).

    >>> import asyncio
    >>> s = PyFilesReader(asyncio)
    >>> assert len(s) > 10  # to test length (currently asyncio has 29 modules
    >>> 'locks.py' in s
    True

    But you can also specify an __init__.py filepath or the directory containing it.

    >>> import os
    >>> init_filepath = asyncio.__file__
    >>> dirpath_to_asyncio_modules = os.path.dirname(init_filepath)
    >>> ss = PyFilesReader(init_filepath)
    >>> sss = PyFilesReader(dirpath_to_asyncio_modules)
    >>> assert list(s) == list(ss) == list(sss)

    """

    def __init__(self, src, *, max_levels=None):
        super().__init__(rootdir=resolve_to_folder(src), max_levels=max_levels)

    def init_file_contents(self):
        """Returns the string of contents of the __init__.py file if it exists, and None if not"""
        return self.get("__init__.py", None)

    def is_pkg(self):
        """Returns True if, and only if, the root is a pkg folder (i.e. has an __init__.py file)"""
        return "__init__.py" in self


# TODO: Make it work
# @py_files_wrap
# class PyFilesText(Files):
#     def __init__(self, src, *, max_levels=None):
#         super().__init__(rootdir=resolve_to_folder(src), max_levels=max_levels)


PkgFilesReader = PyFilesReader  # back-compatibility alias

builtins_rootdir = os.path.dirname(os.__file__)
builtins_py_files = cached_keys(PyFilesReader(builtins_rootdir))

sitepackages_rootdir = next(iter(site.getsitepackages()))
sitepackages_py_files = cached_keys(PyFilesReader(sitepackages_rootdir))


@filt_iter(filt=lambda k: not k.endswith("__pycache__"))
@wrap_kvs(key_of_id=lambda x: x[:-1], id_of_key=lambda x: x + os.path.sep)
@mk_relative_path_store(prefix_attr="rootdir")
class PkgReader(DirCollection, KvReader):
    def __getitem__(self, k):
        return PyFilesReader(os.path.join(self.rootdir, k))
