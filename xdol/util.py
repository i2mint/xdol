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
