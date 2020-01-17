import os
import math
import importlib.util
from tempfile import TemporaryDirectory
from typing import (
    List,
    Optional,
)

import logging
from more_itertools import first

logger = logging.getLogger(__name__)


# The following is adapted from
# https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python/14822210#14822210
def convert_size(size_bytes: float) -> str:
    if size_bytes == 0:
        return "0 B"
    order_of_magnitude = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)

    return f'{s} {order_of_magnitude[i]}'


def get_blacklist() -> List[str]:
    with open('blacklist', 'r') as fp:
        return [line.rstrip('\n') for line in fp]


def file_id(path: str, ext: Optional[str] = None):
    """
    :param path: filepath potentially including preceding directories or file
    extensions
    :param ext: file extension to be removed. If not provided, everything is
    removed after the *first* '.'
    :return: filename without preceding directories or extensions.
    """

    path = os.path.basename(path)
    if ext is None:
        return first(path.split('.', 1))
    else:
        return remove_ext(path, ext)


def remove_ext(path: str, ext: str):
    """
    Remove a file extension. No effect if provided extension is missing.
    """
    if not ext.startswith('.'):
        ext = '.' + ext
    parts = path.rsplit(ext, 1)
    if len(parts) == 2 and parts[1] == '':
        return first(parts)
    else:
        return path


class DirectoryChange:
    """
    Context manager facilitating an undoable temporary switch to another working
    directory.
    """

    def __init__(self, new_dir):
        self.new_dir = new_dir

    def __enter__(self):
        self.old_dir = os.getcwd()
        os.chdir(self.new_dir)
        return self.new_dir

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        os.chdir(self.old_dir)


class TemporaryDirectoryChange(DirectoryChange):
    """
    Directory change context manager that creates and cleans up a temporary
    directory.
    """

    def __init__(self):
        self.tmp = TemporaryDirectory()
        super().__init__(self.tmp.name)

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.tmp.cleanup()


def load_external_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


