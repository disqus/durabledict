# coding=utf-8

import unittest
from mock import Mock

from durabledict.encoding import (
    NoOpEncoding,
    PickleEncoding,
    JSONEncoding,
    EncodingError,
    DecodingError,
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

    def test_raises_encoding_and_decoding_errors(self):
        data = Mock(side_effect=KeyError('hi'))

        with self.assertRaises(EncodingError):
            self.encoding.encode(data)

        with self.assertRaises(DecodingError):
            self.encoding.decode(data)


class NoOpEncodingTest(EncodingTest, unittest.TestCase):
    encoding = NoOpEncoding
    
    def test_raises_encoding_and_decoding_errors(self):
        pass


class PickleEncodingTest(EncodingTest, unittest.TestCase):
    encoding = PickleEncoding


class JSONEncodingTest(EncodingTest, unittest.TestCase):
    encoding = JSONEncoding
