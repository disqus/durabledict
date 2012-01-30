import unittest
from redis import Redis

import modeldict
from modeldict.dict import RedisDict
from modeldict.base import PersistedDict

import unittest
import mock
import time


class BaseTest(object):

    @property
    def dict_class(self):
        return self.dict.__class__.__name__

    def mockmeth(self, method):
        return "modeldict.dict.%s.%s" % (self.dict_class, method)

    def test_acts_like_a_dictionary(self):
        self.dict['foo'] = 'bar'
        self.assertTrue(self.dict['foo'], 'bar')

    def test_setitem_calls_persist_with_value(self):
        with mock.patch(self.mockmeth('persist')) as pv:
            self.dict['foo'] = 'bar'
            pv.assert_called_with('foo', 'bar')

    def test_delitem_calls_depersist(self):
        self.dict['foo'] = 'bar'

        with mock.patch(self.mockmeth('depersist')) as dp:
            del self.dict['foo']
            dp.assert_called_with('foo')

    def test_uses_existing_persistants_on_initialize(self):
        with mock.patch(self.mockmeth('persistants')) as p:
            p.return_value = dict(a=1, b=2, c=3)
            self.assertEquals(self.new_dict(), dict(a=1, b=2, c=3))

    def test_caches_load_from_persistants_until_updated(self):
        self.dict['foo'] = 'bar'

        with mock.patch(self.mockmeth('last_updated')) as last_updated:
            # Make it like the persistants have never been updated
            last_updated.return_value = 0

            with mock.patch(self.mockmeth('persistants')) as persistants:
                # But the persistants have been updated to a new value
                persistants.return_value = dict(updated='persistants')
                self.assertEquals(self.dict, dict(foo='bar'))

                # Change the last updated to a high number to expire the cache,
                # then fetch a new value by calling len() on the dict
                last_updated.return_value = 8000000000
                len(self.dict)
                self.assertEquals(self.dict, dict(updated='persistants'))


class TestRedisDict(BaseTest, unittest.TestCase):

    def new_dict(self):
        return RedisDict('test', Redis())

    def setUp(self):
        self.dict = self.new_dict()

    def tearDown(self):
        self.dict.conn.flushdb()

    def hget(self, key):
        return self.dict.conn.hget('test', key)

    def test_persist_saves_to_redis(self):
        self.assertFalse(self.hget('foo'))
        self.dict.persist('foo', 'bar')
        self.assertEquals(self.hget('foo'), 'bar')

    def test_depersist_removes_it_from_redis(self):
        self.dict['foo'] = 'bar'
        self.assertEquals(self.hget('foo'), 'bar')
        self.dict.depersist('foo')
        self.assertFalse(self.hget('foo'))

    def test_persistants_returns_items_in_redis(self):
        self.dict.persist('foo', 'bar')
        self.dict.persist('baz', 'bang')

        new_dict = RedisDict('test', Redis())

        self.assertEquals(new_dict.persistants(), dict(foo='bar', baz='bang'))

    def test_last_updated_set_on_initialize(self):
        self.assertEquals(self.dict.last_updated(), 1)

    def test_last_updated_set_when_persisted(self):
        before = self.dict.last_updated()

        time.sleep(0.01)
        self.dict.persist('foo', 'bar')
        self.assertTrue(self.dict.last_updated() > before)

    def test_last_updated_set_when_depersisted(self):
        self.dict.persist('foo', 'bar')
        before = self.dict.last_updated()

        time.sleep(0.01)
        self.dict.depersist('foo')
        self.assertTrue(self.dict.last_updated() > before)

    def test_persists_and_last_update_writes_are_atomic(self):
        with mock.patch('redis.Redis.incr') as tlo:
            tlo.side_effect = Exception('boom!')
            self.assertRaisesRegexp(Exception, 'boom',
                                    self.dict.persist, 'foo', 'bar')
            self.assertFalse(self.hget('foo'))
