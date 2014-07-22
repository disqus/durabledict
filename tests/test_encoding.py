# coding=utf-8

import unittest

from durabledict.encoding import (
    NoOpEncoding,
    PickleEncoding,
    JSONEncoding
)


class EncodingTest(object):

    def cycle(self, data):
        return self.encoding.decode(self.encoding.encode(data))

    def test_encodes_basic_objects(self):
        self.assertEqual(7, self.cycle(7))
        self.assertEqual("seven", self.cycle("seven"))
        self.assertEqual([1, 2, 3], self.cycle([1, 2, 3]))
        # XXX: The JSONEncoding converts all keys to strings, so test with a
        #      compatable dict here.
        self.assertEqual({"1": 2}, self.cycle({"1": 2}))


class NoOpEncodingTest(EncodingTest, unittest.TestCase):
    encoding = NoOpEncoding


class PickleEncodingTest(EncodingTest, unittest.TestCase):
    encoding = PickleEncoding


class JSONEncodingTest(EncodingTest, unittest.TestCase):
    encoding = JSONEncoding
