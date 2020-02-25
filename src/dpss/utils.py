import gzip
import os
import math
import importlib.util
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import (
    Optional,
    Union,
    Generator,
    TypeVar,
    Iterable,
    Type,
    Callable,
    Any,
    Tuple,
    List,
    Collection,
)

from more_itertools import (
    first,
    one,
)

Module = type(math)


# The following is adapted from
# https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python/14822210#14822210
def convert_size(size_bytes: float) -> str:
    if size_bytes == 0:
        return "0 B"
    elif size_bytes < 0:
        size_bytes = -size_bytes
        sign = '-'
    else:
        sign = ''
    order_of_magnitude = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    d = round(size_bytes / p, 2)

    return f'{sign}{d} {order_of_magnitude[i]}'


def common_attr(objects, attr):
    """
    Assert that the specified attribute is the same across all objects and
    return its unique value.
    """
    return one({getattr(o, attr) for o in objects})


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


def gunzip(gzfilename: Union[str, Path], rm_gz: bool = True) -> None:
    """
    Uncompress a gzip-d file on disk.
    :param gzfilename: gzip-ed file to uncompress.
    :param rm_gz: whether to delete the compressed file after uncompressing.
    """
    gzfilename = str(gzfilename)
    filename = remove_ext(gzfilename, '.gz')
    with gzip.open(gzfilename, 'rb') as gzfile:
        with open(filename, 'wb') as outfile:
            shutil.copyfileobj(gzfile, outfile)
    if rm_gz:
        os.remove(gzfilename)


def traverse_dirs(root: Path, follow_symlinks: bool = False) -> Generator[Path, None, None]:
    """
    Iterate all directories starting from the specified root.
    :return:
    """
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


def load_external_module(name, path) -> Module:
    """
    Dynamically load a module from a non-project file.
    Probably a bad idea.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


T = TypeVar('T')


def filter_exceptions(
    func: Callable[[T], Any],
    items: Iterable[T],
    exc_cls: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception
) -> Tuple[List[T], List[Tuple[T, Exception]]]:
    """
    Filter an iterable based on the exceptions raised while processing it.
    :param func: exception-raising function to be mapped across the items
    :param items: objects to be passed to the raising function
    :param exc_cls: exception type(s) to be caught. Defaults to `Exception`.
    :return: iterator of items in `items` that did not raise exceptions;
    dictionary mapping those that did raise to the exceptions they raised.
    """

    non_raising_items = []
    raising_items = []

    for x in items:
        try:
            func(x)
        except exc_cls as exc:
            raising_items.append((x, exc))
        else:
            non_raising_items.append(x)

    return non_raising_items, raising_items


def sort_optionals(items: Collection, none_behavior: str = 'back', **kwargs):
    not_none = [x for x in items if x is not None]
    none = [None] * (len(items) - len(not_none))
    not_none.sort(**kwargs)
    if none_behavior == 'front':
        return none + not_none
    elif none_behavior == 'back':
        return not_none + none
    elif none_behavior == 'stable':
        raise NotImplementedError
    else:
        raise ValueError(none_behavior)
