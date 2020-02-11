#!/usr/bin/env python3

import os
from tempfile import TemporaryDirectory
import unittest

from attr import dataclass

from dpss.utils import (
    convert_size,
    file_id,
    DirectoryChange,
    remove_ext,
    common_attr,
    filter_exceptions,
    sort_optionals,
)


class TestUtils(unittest.TestCase):

    def test_convert_size(self):
        for size_bytes, size_str in [
            (0, '0 B'),
            (1024, '1.0 KB'),
            (2124569754, '1.98 GB')
        ]:
            self.assertEqual(convert_size(size_bytes), size_str)

    def test_file_id(self):
        for args, extracted_id in [
            (('/fish/dish',), 'dish'),
            (('dish.exe',), 'dish'),
            (('foo/bar/baz.egg',), 'baz'),
            (('dotted.dir/file',), 'file'),
            (('eggs.ham.spam',), 'eggs'),
            (('eggs.ham.spam', 'spam'), 'eggs.ham'),
            (('eggs/ham.spam', 'foo'), 'ham.spam')
        ]:
            self.assertEqual(file_id(*args), extracted_id)

    def test_remove_ext(self):
        for file, ext, basename in [
            ('x.zip', '.zip', 'x'),
            ('x.zip', '.whoops', 'x.zip'),
            ('x/y/z.ship.zip', '.zip', 'x/y/z.ship'),
            ('x/y/z.ship.zip', '.ship', 'x/y/z.ship.zip')
        ]:
            self.assertEqual(remove_ext(file, ext), basename)

    def test_directory_change(self):
        old_dir = os.getcwd()
        with TemporaryDirectory() as test_dir_1, TemporaryDirectory() as test_dir_2:
            with DirectoryChange(test_dir_1) as new_dir:
                self.assertEqual(new_dir, test_dir_1)
                os.chdir(test_dir_2)  # go somewhere else
        self.assertEqual(os.getcwd(), old_dir)

    def test_common_attr(self):
        @dataclass
        class A:
            a: int

        self.assertEqual(common_attr([A(6) for _ in range(10)], 'a'), 6)
        self.assertRaises(ValueError, common_attr, [A(1), A(0)], 'a')

    def test_filter_exceptions(self):
        with self.subTest('none raise'):
            self.assertEqual(filter_exceptions(lambda a: a, [1, 2, 3]), ([1, 2, 3], []))

        with self.subTest('some raise'):
            passed, failed = filter_exceptions(lambda x: 1/x, [0, 1])
            self.assertEqual(passed, [1])
            self.assertEqual(len(failed), 1)
            self.assertEqual(failed[0][0], 0)
            self.assertTrue(isinstance(failed[0][1], ZeroDivisionError))

        with self.subTest('non target raise'):
            self.assertRaises(ZeroDivisionError, filter_exceptions, lambda x: 1/x, [0, 1], AttributeError)

    def test_sort_optionals(self):
        for unordered, ordered, behavior in [
            ([3, 2, 1], [1, 2, 3], 'back'),
            ([None, 3, None, 2, None, 1], [1, 2, 3, None, None, None], 'back'),
            ([2, 1, 3, None], [None, 1, 2, 3], 'front')
        ]:
            self.assertEqual(sort_optionals(unordered, behavior), ordered)


if __name__ == '__main__':
    unittest.main()
