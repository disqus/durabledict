import unittest
from redis import Redis

import modeldict
from modeldict.dict import RedisDict, ModelDict
from modeldict.base import PersistedDict
from tests.models import Setting

import unittest
import mock
import time

import django.core.management
from django.core.cache.backends.locmem import LocMemCache


class BaseTest(object):

    @property
    def dict_class(self):
        return self.dict.__class__.__name__

    def mockmeth(self, method):
        return "modeldict.dict.%s.%s" % (self.dict_class, method)

    def assertDictAndPersistantsHave(self, **kwargs):
        self.assertEquals(self.dict, kwargs)
        self.assertEquals(self.dict.persistants(), kwargs)

    def test_acts_like_a_dictionary(self):
        self.dict['foo'] = 'bar'
        self.assertEquals(self.dict['foo'], 'bar')

        self.dict['foo2'] = 'bar2'
        self.assertEquals(self.dict['foo2'], 'bar2')

    def test_updates_dict_keys(self):
        self.dict['foo'] = 'bar'
        self.assertEquals(self.dict['foo'], 'bar')
        self.dict['foo'] = 'newbar'
        self.assertEquals(self.dict['foo'], 'newbar')

    def test_raises_keyerror_on_bad_access(self):
        self.dict['foo'] = 'bar'
        self.dict.__getitem__('foo')
        del self.dict['foo']
        self.assertRaises(KeyError, self.dict.__getitem__, 'foo')

    def test_setitem_calls_persist_with_value(self):
        with mock.patch(self.mockmeth('persist')) as pv:
            self.dict['foo'] = 'bar'
            pv.assert_called_with('foo', 'bar')

    def test_saving_non_strings_saves_the_str_of_the_object(self):
        instance = Setting.objects.create(key='nk', value='nv')
        self.dict['foo'] = instance
        self.assertEquals(str(instance), self.dict['foo'])

    def test_setdefault_works_and_persists_correctly(self):
        self.assertFalse(self.dict.get('foo'))

        self.assertTrue(self.dict.setdefault('foo', 'bar'), 'bar')
        self.assertTrue(self.dict.setdefault('foo', 'notset'), 'bar')

        self.assertDictAndPersistantsHave(foo='bar')
        self.assertEquals(self.dict['foo'], 'bar')

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

    def test_last_updated_setup_on_intialize(self):
        self.assertTrue(self.dict.last_updated())

    def test_last_updated_set_when_persisted(self):
        before = self.dict.last_updated()

        self.dict.persist('foo', 'bar')
        self.assertTrue(self.dict.last_updated() > before)

    def test_last_updated_set_when_depersisted(self):
        self.dict.persist('foo', 'bar')
        before = self.dict.last_updated()

        self.dict.depersist('foo')
        self.assertTrue(self.dict.last_updated() > before)

    def test_calling_update_raises_notimplemented(self):
        self.assertRaises(NotImplementedError, self.dict.update())

    def test_pop_works_correctly(self):
        self.dict.persist('foo', 'bar')
        self.dict.persist('buz', 'buffle')
        self.assertDictAndPersistantsHave(foo='bar', buz='buffle')

        self.assertEquals(self.dict.pop('buz', 'keynotfound'), 'buffle')
        self.assertDictAndPersistantsHave(foo='bar')

        self.assertEquals(self.dict.pop('junk', 'keynotfound'), 'keynotfound')
        self.assertDictAndPersistantsHave(foo='bar')

        self.assertEquals(self.dict.pop('foo'), 'bar')
        self.assertDictAndPersistantsHave()

        self.assertEquals(self.dict.pop('no_more_keys', 'default'), 'default')
        self.assertRaises(KeyError, self.dict.pop, 'no_more_keys')

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

    def test_last_updated_set_to_1_on_initialize(self):
        self.assertEquals(self.dict.last_updated(), 1)

    def test_persists_and_last_update_writes_are_atomic(self):
        with mock.patch('redis.Redis.incr') as tlo:
            tlo.side_effect = Exception('boom!')
            self.assertRaisesRegexp(Exception, 'boom',
                                    self.dict.persist, 'foo', 'bar')
            self.assertFalse(self.hget('foo'))

class TestModelDict(BaseTest, unittest.TestCase):

    @property
    def cache(self):
        return LocMemCache('test', {})

    def new_dict(self):
        return ModelDict(Setting.objects, key_col='key', cache=self.cache)

    def setUp(self):
        self.dict = self.new_dict()
        django.core.management.call_command('syncdb')

    def tearDown(self):
        django.core.management.call_command('flush', interactive=False)
        self.dict.cache.clear()

    def test_persist_saves_model(self):
        self.dict.persist('foo', 'bar')
        self.assertTrue(Setting.objects.get(key='foo'), 'bar')

    def test_depersist_removes_model(self):
        self.dict.persist('foo', 'bar')
        self.assertTrue(Setting.objects.get(key='foo'), 'bar')
        self.dict.depersist('foo')
        self.assertRaises(Setting.DoesNotExist, Setting.objects.get, key='foo')

    def test_persistants_returns_dict_of_models_in_db(self):
        self.dict.persist('foo', 'bar')
        self.dict.persist('buzz', 'bang')
        self.assertEquals(self.dict.persistants(), dict(foo='bar', buzz='bang'))

    def test_last_updated_set_to_1_on_initialize(self):
        self.assertEquals(self.dict.last_updated(), 1)

    def test_setdefault_does_not_update_last_updated_if_key_exists(self):
        self.dict.persist('foo', 'bar')
        before = self.dict.last_updated()

        self.dict.setdefault('foo', 'notset')
        self.assertEquals(before, self.dict.last_updated())

    def test_changes_to_last_updated_are_atomic(self):
        pass