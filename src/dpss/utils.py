import gzip
import logging
import os
import math
import importlib.util
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import (
    Optional,
    Union,
)

from more_itertools import first


def setup_log(log_name, log_level, *handlers):
    log = logging.getLogger(log_name)
    log.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    for handler in handlers:
        handler.setFormatter(formatter)
        log.addHandler(handler)


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


def file_id(path: Union[str, Path], ext: Optional[str] = None):
    """
    :param path: filepath potentially including preceding directories or file
    extensions
    :param ext: file extension to be removed. If not provided, everything is
    removed after the *first* '.'
    :return: filename without preceding directories or extensions.
    """

    basename = path.name if isinstance(path, Path) else os.path.basename(path)
    if ext is None:
        return first(basename.split('.', 1))
    else:
        return remove_ext(basename, ext)


def remove_ext(filename: str, ext: str) -> str:
    """
    Remove a file extension. No effect if provided extension is missing.
    """
    if not ext.startswith('.'):
        ext = f'.{ext}'
    parts = filename.rsplit(ext, 1)
    if len(parts) == 2 and parts[1] == '':
        return parts[0]
    else:
        return filename


def gunzip(gzfilename: Union[str, Path], rm_gz: bool = True):
    gzfilename = str(gzfilename)
    filename = remove_ext(gzfilename, '.gz')
    with gzip.open(gzfilename, 'rb') as gzfile:
        with open(filename, 'wb') as outfile:
            shutil.copyfileobj(gzfile, outfile)
    if rm_gz:
        os.remove(gzfilename)


def traverse_dirs(root: Path, follow_symlinks: bool = False):
    yield root
    for entry in root.iterdir():
        if entry.is_dir() and (follow_symlinks or not entry.is_symlink()):
            for d in traverse_dirs(entry, follow_symlinks):
                yield d


class DirectoryChange:
    """
    Context manager facilitating an undoable temporary switch to another working
    directory.
    """

    def __init__(self, new_dir):
        self.new_dir = new_dir

    def __enter__(self):
        self.old_dir = Path.cwd()
        os.chdir(self.new_dir)
        return self.new_dir

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        os.chdir(str(self.old_dir))


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
