# coding=utf-8

import unittest
import mock

from redis import Redis
from durabledict import RedisDict, ModelDict, MemoryDict, ZookeeperDict
from tests.models import Setting

from contextlib import contextmanager

import django.core.management
from django.core.cache.backends.locmem import LocMemCache

from kazoo.client import KazooClient

import threading
import thread


class BaseTest(object):

    @property
    def dict_class(self):
        return self.dict.__class__.__name__

    def setUp(self):
        super(BaseTest, self).setUp()
        self.keyspace = 'test'
        self.dict = self.new_dict()

    def mockmeth(self, method):
        return "durabledict.%s.%s" % (self.dict_class, method)

    def assertDictAndPersistantsHave(self, **kwargs):
        self.assertEquals(self.dict, kwargs)
        self.assertEquals(self.dict.durables(), kwargs)

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

    def test_can_save_and_retrieve_complex_objects(self):
        complex_vars = ('tuple', 'fun', 1.0, [1, 2, 3, 4], u'☃')
        self.dict['foo'] = complex_vars

        self.assertEquals(complex_vars, self.dict['foo'])
        self.assertEquals(dict(foo=complex_vars), self.dict.durables())

        self.assertEquals(self.dict.setdefault('foo', complex_vars), complex_vars)
        self.assertEquals(self.dict.setdefault('bazzle', 'fuzzle'), 'fuzzle')

        self.assertEquals(self.dict.pop('foo'), complex_vars)

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

    def test_uses_existing_durables_on_initialize(self):
        with mock.patch(self.mockmeth('durables')) as p:
            p.return_value = dict(a=1, b=2, c=3)
            self.assertEquals(self.new_dict(), dict(a=1, b=2, c=3))

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
        self.dict['foo'] = 'bar'
        self.dict['buz'] = 'buffle'
        self.assertDictAndPersistantsHave(foo='bar', buz='buffle')

        self.assertEquals(self.dict.pop('buz', 'keynotfound'), 'buffle')
        self.assertDictAndPersistantsHave(foo='bar')

        self.assertEquals(self.dict.pop('junk', 'keynotfound'), 'keynotfound')
        self.assertDictAndPersistantsHave(foo='bar')

        self.assertEquals(self.dict.pop('foo'), 'bar')
        self.assertDictAndPersistantsHave()

        self.assertEquals(self.dict.pop('no_more_keys', 'default'), 'default')
        self.assertRaises(KeyError, self.dict.pop, 'no_more_keys')

    def test_get_works_correctly(self):
        self.dict['foo'] = 'bar'
        self.assertEquals('bar', self.dict.get('foo'))
        self.assertEquals('default', self.dict.get('junk', 'default'))
        self.assertEquals(None, self.dict.get('junk'))

    def test_get_does_not_remove_element(self):
        self.dict['foo'] = 'bar'
        self.assertEquals('bar', self.dict.get('foo'))
        self.assertEquals('bar', self.dict.get('foo'))

    def test_contains_works(self):
        self.assertFalse('foo' in self.dict)
        self.dict['foo'] = 'bar'
        self.assertTrue('foo' in self.dict)
        self.assertFalse('bar' in self.dict)

    def test_uses_custom_encoding_passed_in(self):
        class SevenEncoding(object):
            encode = staticmethod(lambda d: '7')
            decode = staticmethod(lambda d: d)

        sevens_dict = self.new_dict(encoding=SevenEncoding)
        sevens_dict['key'] = 'this is not a seven'
        self.assertEquals('7', sevens_dict.get('key'))


class AutoSyncTrueTest(object):

    def test_does_not_update_self_until_persistats_have_updated(self):
        self.dict['foo'] = 'bar'

        with mock.patch(self.mockmeth('last_updated')) as last_updated:
            # Make it like the durables have never been updated
            last_updated.return_value = 0

            with mock.patch(self.mockmeth('durables')) as durables:
                # But the durables have been updated to a new value
                durables.return_value = dict(updated='durables')
                self.assertEquals(self.dict, dict(foo='bar'))

                # Change the last updated to a high number to expire the cache,
                # then fetch a new value by calling len() on the dict
                last_updated.return_value = 8000000000
                len(self.dict)
                self.assertEquals(self.dict, dict(updated='durables'))


class AutoSyncFalseTest(object):

    def test_does_not_update_from_durables_on_read(self):
        # Add a key to the dict, which will sync with durables
        self.assertEquals(self.dict, dict())
        self.dict['foo'] = 'bar'

        # Manually add a value not using the public API
        self.dict.persist('added', 'value')

        # And it should not be there
        self.assertEquals(self.dict, dict(foo='bar'))

        # Now sync and see that it's updated
        self.dict.sync()
        self.assertDictAndPersistantsHave(foo='bar', added='value')


class RedisTest(object):

    def tearDown(self):
        self.dict.connection.flushdb()
        super(RedisTest, self).tearDown()

    def hget(self, key):
        return self.dict.connection.hget(self.keyspace, key)

    def test_instances_different_keyspaces_do_not_share_last_updated(self):
        self.dict['foo'] = 'bar'
        self.dict['bazzle'] = 'bungie'
        self.assertEquals(self.dict.last_updated(), 3)

        new_dict = self.new_dict(keyspace='another_one')
        self.assertNotEquals(self.dict.last_updated(), new_dict.last_updated())


class ModelDictTest(object):

    def tearDown(self):
        django.core.management.call_command('flush', interactive=False)
        self.dict.cache.clear()
        super(ModelDictTest, self).tearDown()

    def setUp(self):
        django.core.management.call_command('syncdb')
        super(ModelDictTest, self).setUp()

    @property
    def cache(self):
        return LocMemCache(self.keyspace, {})


class ZookeeperDictTest(object):

    namespace = '/durabledict/test'

    def set_event_when_updated(self, client, value, event):
        while client.last_updated() is not value:
            pass

        event.set()

    def new_client(self):
        client = KazooClient(hosts='zookeeper:2181')
        client.start()
        return client

    @contextmanager
    def wait_for_update_of(self, zkdict, value):
        event = threading.Event()
        args = (zkdict, value, event)

        thread.start_new_thread(self.set_event_when_updated, args)

        event.wait(timeout=5)
        actual = zkdict.last_updated()
        format = 'Gave up waiting for last_updated value of %s, last saw %s'

        self.assertEquals(actual, value, format % (actual, value))

        yield

    def test_other_cluster_in_namespace_does_not_effect_main_one(self):
        other_dict = ZookeeperDict(keyspace='/durabledict/other', connection=self.new_client())

        self.dict['foo'] = 'dict bar'
        self.dict['dictkey'] = 'dict'

        other_dict['foo'] = 'other'
        other_dict['otherdictkey'] = 'otherv'

        # This is "5" and not "3" because of a bug.  When a ZKDict object has
        # __setattr__, it updates the last_updated value itself, PLUS the
        # ZKDict's child watch sees that change and also updates the
        # last_updated value as well.  This isn't ideal and will get changed.
        with self.wait_for_update_of(other_dict, 5):
            self.assertEquals(
                self.dict,
                dict(foo='dict bar', dictkey='dict')
            )
            self.assertEquals(
                other_dict,
                dict(foo='other', otherdictkey='otherv')
            )

    def test_changes_by_one_dict_are_reflected_in_another(self):
        other_dict = ZookeeperDict(keyspace=self.namespace, connection=self.new_client())

        self.assertEquals(other_dict, {})

        self.dict['foo'] = 'bar'
        self.dict['baz'] = 'bub'

        with self.wait_for_update_of(other_dict, 3):
            self.assertEquals(other_dict['foo'], 'bar')
            self.assertEquals(other_dict['baz'], 'bub')

        self.dict['foo'] = 'changed'

        with self.wait_for_update_of(other_dict, 4):
            self.assertEquals(other_dict['foo'], 'changed')
            self.assertEquals(other_dict['baz'], 'bub')

    def test_raises_valueerror_for_keys_with_strings(self):
        self.assertRaises(
            ValueError,
            self.dict.__setitem__,
            'with/slash',
            'value'
        )

    def test_starts_the_zk_client_if_not_already_started(self):
        client = mock.Mock(connected=False)

        ZookeeperDict(keyspace=self.namespace, connection=client)
        client.start.assert_called_once_with()

        client.start.reset_mock()
        client.connected = True
        ZookeeperDict(keyspace=self.namespace, connection=client)
        self.assertFalse(client.start.called)


class TestRedisDict(BaseTest, AutoSyncTrueTest, RedisTest, unittest.TestCase):

    def new_dict(self, keyspace=None, **kwargs):
        return RedisDict(keyspace=(keyspace or self.keyspace), connection=Redis(host='redis'), **kwargs)

    def test_persist_saves_to_redis(self):
        self.assertFalse(self.hget('foo'))
        self.dict.persist('foo', 'bar')
        self.assertEquals(self.dict.encoding.decode(self.hget('foo')), 'bar')

    def test_depersist_removes_it_from_redis(self):
        self.dict['foo'] = 'bar'
        self.assertEquals(self.dict.encoding.decode(self.hget('foo')), 'bar')
        self.dict.depersist('foo')
        self.assertFalse(self.hget('foo'))

    def test_durables_returns_items_in_redis(self):
        self.dict.persist('foo', 'bar')
        self.dict.persist('baz', 'bang')

        new_dict = self.new_dict()

        self.assertEquals(new_dict.durables(), dict(foo='bar', baz='bang'))

    def test_last_updated_set_to_1_on_initialize(self):
        self.assertEquals(self.dict.last_updated(), 1)

    def test_persists_and_last_update_writes_are_atomic(self):
        with mock.patch('redis.Redis.incr') as tlo:
            tlo.side_effect = Exception('boom!')
            self.assertRaisesRegexp(Exception, 'boom',
                                    self.dict.persist, 'foo', 'bar')
            self.assertFalse(self.hget('foo'))

    def test_can_instantiate_without_keywords(self):
        RedisDict(self.keyspace, Redis(host='redis'))


class TestModelDict(BaseTest, AutoSyncTrueTest, ModelDictTest, unittest.TestCase):

    def new_dict(self, **kwargs):
        return ModelDict(manager=Setting.objects, cache=self.cache, key_col='key', **kwargs)

    def test_can_be_constructed_with_return_instances(self):
        instance = ModelDict(manager=Setting.objects, cache=self.cache, return_instances='ri')
        self.assertEquals(
            instance.return_instances,
            'ri'
        )

    def test_persist_saves_model(self):
        self.dict.persist('foo', 'bar')
        self.assertEquals(self.dict['foo'], 'bar')

    def test_depersist_removes_model(self):
        self.dict.persist('foo', 'bar')
        self.assertTrue(self.dict['foo'], 'bar')
        self.dict.depersist('foo')
        self.assertRaises(Setting.DoesNotExist, Setting.objects.get, key='foo')

    def test_durables_returns_dict_of_models_in_db(self):
        self.dict.persist('foo', 'bar')
        self.dict.persist('buzz', 'bang')
        self.assertEquals(self.dict.durables(), dict(foo='bar', buzz='bang'))

    def test_last_updated_set_to_1_on_initialize(self):
        self.assertEquals(self.dict.last_updated(), 1)

    def test_setdefault_does_not_update_last_updated_if_key_exists(self):
        self.dict.persist('foo', 'bar')
        before = self.dict.last_updated()

        self.dict.setdefault('foo', 'notset')
        self.assertEquals(before, self.dict.last_updated())

    def test_instances_true_returns_the_whole_object_at_they_key(self):
        self.dict.persist('foo', 'bar')
        self.dict.return_instances = True
        self.assertEquals(self.dict['foo'], Setting.objects.get(key='foo'))

    def test_touch_last_updated_inc_cache(self):
        self.dict.cache = mock.Mock()
        self.dict.touch_last_updated()
        self.dict.cache.incr.assert_called_once_with(self.dict.cache_key)

    def test_resets_last_update_if_value_is_deleted(self):
        self.dict.persist('foo', 'bar')
        self.assertEquals(self.dict['foo'], 'bar')

        self.dict.cache.delete(self.dict.cache_key)

        self.dict.persist('baz', 'fizzle')
        self.assertEquals(self.dict['foo'], 'bar')
        self.assertEquals(self.dict['baz'], 'fizzle')
        self.assertEquals(
            self.dict.cache.get(self.dict.cache_key),
            self.dict.LAST_UPDATED_MISSING_INCREMENT + 2  # for 2 updates
        )


class TestMemoryDict(BaseTest, AutoSyncTrueTest, unittest.TestCase):

    def new_dict(self, *args, **kwargs):
        return MemoryDict(*args, **kwargs)

    def test_does_not_pickle_objects_when_set(self):
        obj = object()
        self.dict['foo'] = obj

        self.assertEquals(self.dict.values(), [obj])


class TestZookeeperDict(BaseTest, ZookeeperDictTest, unittest.TestCase):

    def new_dict(self, **kwargs):
        client = KazooClient(hosts='zookeeper:2181')
        client.start()
        client.delete(self.namespace, recursive=True)
        return ZookeeperDict(keyspace=self.namespace, connection=client, **kwargs)




class TestRedisDictManualSync(BaseTest, RedisTest, AutoSyncFalseTest, unittest.TestCase):

    def new_dict(self, keyspace=None, **kwargs):
        return RedisDict(keyspace=(keyspace or self.keyspace), connection=Redis(host='redis'), autosync=False, **kwargs)


class TestModelDictManualSync(BaseTest, ModelDictTest, AutoSyncFalseTest, unittest.TestCase):

    def new_dict(self, **kwargs):
        return ModelDict(manager=Setting.objects, cache=self.cache, key_col='key', autosync=False, **kwargs)


class TestZookeeperDictManualSync(BaseTest, ZookeeperDictTest, unittest.TestCase, ):

    def new_dict(self, **kwargs):
        client = KazooClient(hosts='zookeeper:2181')
        client.start()
        client.delete(self.namespace, recursive=True)
        return ZookeeperDict(keyspace=self.namespace, connection=client, autosync=False, **kwargs)
