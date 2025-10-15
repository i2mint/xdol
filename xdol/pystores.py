"""Stores for python objects"""

import site
import os
from functools import wraps
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
    @wraps(DirCollection.__init__)
    def __init__(self, rootdir, *args, **kwargs):
        super().__init__(rootdir=resolve_to_folder(rootdir), *args, **kwargs)

    def __getitem__(self, k):
        k = os.path.join(self.rootdir, k)
        return PyFilesReader(k)


def _is_setup_cfg(filepath: str) -> bool:
    """Check if filepath is a setup.cfg file.

    >>> _is_setup_cfg('project/setup.cfg')
    True
    >>> _is_setup_cfg('project/pyproject.toml')
    False
    """
    return os.path.basename(filepath) == 'setup.cfg'


@filt_iter(filt=_is_setup_cfg)
@mk_relative_path_store(prefix_attr="rootdir")
class SetupCfgReader(FileBytesReader, KvReader):
    """Mapping interface to setup.cfg files in a directory tree.
    Keys are relative paths to setup.cfg files.
    Values are the string contents of the setup.cfg files.

    >>> import tempfile, os
    >>> with tempfile.TemporaryDirectory() as tmpdir:
    ...     # Create some setup.cfg files
    ...     proj1_dir = os.path.join(tmpdir, 'proj1')
    ...     os.makedirs(proj1_dir)
    ...     cfg_path = os.path.join(proj1_dir, 'setup.cfg')
    ...     with open(cfg_path, 'w') as f:
    ...         _ = f.write('[options]\\ninstall_requires = requests')
    ...
    ...     reader = SetupCfgReader(tmpdir)
    ...     assert len(reader) == 1
    ...     assert 'proj1/setup.cfg' in reader
    ...     assert 'requests' in reader['proj1/setup.cfg']
    True
    """

    def __init__(self, src, *, max_levels=None):
        """Initialize with source directory.

        Args:
            src: Directory path, module, or any source resolvable to a folder
            max_levels: Maximum directory depth to search
        """
        super().__init__(rootdir=resolve_to_folder(src), max_levels=max_levels)

    def __getitem__(self, key):
        """Get setup.cfg content as string, handling decode errors gracefully."""
        try:
            return super().__getitem__(key).decode('utf-8')
        except UnicodeDecodeError:
            return ""  # Return empty string for undecodable files

    def dependencies_from_all(self):
        """Generate all dependencies from all setup.cfg files.

        Yields:
            Individual dependency strings from all setup.cfg files
        """

        def _extract_from_file(cfg_content: str):
            # Import here to avoid circular imports if needed
            from config2py import ConfigReader

            try:
                config = ConfigReader(cfg_content)
                install_requires = config.get('options', {}).get('install_requires', '')
                if isinstance(install_requires, str) and install_requires:
                    for line in install_requires.strip().splitlines():
                        line = line.strip()
                        if line and not line.startswith('#'):
                            yield line
            except Exception:
                # Skip malformed config files
                pass

        for cfg_content in self.values():
            yield from _extract_from_file(cfg_content)
