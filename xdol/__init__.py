"""Extended tools for dol (Data Object Layers)"""

from xdol.pystores import PyFilesReader, PkgReader, SetupCfgReader
from xdol.util import resolve_module_filepath, resolve_to_folder, save_obj
from xdol.updating import update_with_policy
from xdol.x_codecs import resolve_values_to_bytes, Codec, get_codec, paired_store
from xdol.keys import robust_key, make_key_func, UnstableKeyError
from xdol.registry import (
    Registry,
    RegistryError,
    RegistryConflict,
    RegistryMissing,
    Subscription,
)
from xdol.memoize import (
    memoize_calls,
    file_cache_store,
    CacheMiss,
    CacheCorrupt,
    CacheWriteError,
)
