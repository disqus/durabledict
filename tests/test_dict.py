from modeldict.dict import RedisDict
import unittest
from redis import Redis


class TestRedisDict(unittest.TestCase):

    def setUp(self):
        self.dict = RedisDict('test', Redis())

    def tearDown(self):
        self.dict.conn.flushdb()

    def test_setitem_saves_to_redis(self):
        self.assertFalse(self.dict.conn.get('foo'))
        self.dict['foo'] = 'bar'
        self.assertEquals(self.dict['foo'], 'bar')

    def test_delitem_removes_it_from_redis(self):
        self.dict['foo'] = 'bar'
        self.assertEquals(self.dict['foo'], 'bar')
        self.assertFalse(self.dict.conn.get('foo'))
        self.dict['foo'] = 'bar'

    def test_persistants_returns_dict_of_items_in_redis(self):
        self.assertEquals(self.dict, {})
        self.dict['foo'] = 'bar'
        self.dict['baz'] = 'bang'

        new_dict = RedisDict('test', Redis())
        self.assertEquals(new_dict, dict(foo='bar', baz='bang'))
