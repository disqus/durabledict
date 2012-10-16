from modeldict.base import PersistedDict

from kazoo.exceptions import NoNodeError
from functools import wraps
import posixpath


class RedisDict(PersistedDict):
    """
    Dictionary-style access to a redis hash table. Populates a cache and a local
    in-memory to avoid multiple hits to the database.

    Functions just like you'd expect it::

        mydict = RedisDict('my_redis_key', Redis())
        mydict['test']
        >>> 'bar' #doctest: +SKIP
    """
    def __init__(self, keyspace, connection, *args, **kwargs):
        self.keyspace = keyspace
        self.conn = connection
        super(RedisDict, self).__init__(*args, **kwargs)
        self.__touch_last_updated()

    def persist(self, key, value):
        encoded = self._encode(value)
        self.__touch_and_multi(('hset', (self.keyspace, key, encoded)))

    def depersist(self, key):
        self.__touch_and_multi(('hdel', (self.keyspace, key)))

    def persistents(self):
        encoded = self.conn.hgetall(self.keyspace)
        tuples = [(k, self._decode(v)) for k, v in encoded.items()]
        return dict(tuples)

    def last_updated(self):
        return int(self.conn.get(self.__last_update_key) or 0)

    # TODO: setdefault always touches the last_updated value, even if the key
    # existed already.  It should only touch last_updated if the key did not
    # already exist
    def _setdefault(self, key, default=None):
        encoded = self.__touch_and_multi(
            ('hsetnx', (self.keyspace, key, self._encode(default))),
            ('hget', (self.keyspace, key)),
            returns=-1
        )
        return self._decode(encoded)

    def _pop(self, key, default=None):
        last_updated, encoded, key_existed = self.__touch_and_multi(
            ('hget', (self.keyspace, key)),
            ('hdel', (self.keyspace, key))
        )

        if key_existed:
            return self._decode(encoded)
        elif default:
            return default
        else:
            raise KeyError

    def __touch_and_multi(self, *args, **kwargs):
        """
        Runs each tuple tuple of (redis_cmd, args) in provided inside of a Redis
        MULTI block, plus an increment of the last_updated value, then executes
        the MULTI block.  If ``returns`` is specified, it returns that index
        from the results list.  If ``returns`` is None, returns all values.
        """

        with self.conn.pipeline() as pipe:
            pipe.incr(self.__last_update_key)
            [getattr(pipe, function)(*args) for function, args in args]
            results = pipe.execute()

            if kwargs.get('returns'):
                return results[kwargs.get('returns')]
            else:
                return results

    def __touch_last_updated(self):
        return self.conn.incr(self.__last_update_key)

    @property
    def __last_update_key(self):
        return self.keyspace + 'last_updated'


class ModelDict(PersistedDict):
    """
    Dictionary-style access to a model. Populates a cache and a local in-memory
    to avoid multiple hits to the database.

        # Given ``Model`` that has a column named ``foo`` where the value at
        # that column is "bar":

        mydict = ModelDict(Model.manager, value_col='foo')
        mydict['test']
        >>> 'bar' #doctest: +SKIP

    The first positional argument to ``ModelDict`` is ``manager``, which is an
    instance of a Manager which ``ModelDict`` uses to read and write to your
    database.  Any object that conforms to the interface can work, but the
    expectation is that ``manager`` is a Django.model manager.

    If you want to use another key in the ModelDict besides the ``Model``s
    ``pk``, you may specify that in the constructor with ``key_col``.  For
    instance, if your ``Model`` has a column called ``id``, you can index into
    that column by passing ``key_col='id'`` in to the contructor:

        mydict = ModelDict(Model, key_col='id', value_col='foo')
        mydict['test']
        >>> 'bar' #doctest: +SKIP

    The constructor also takes a ``cache`` keyword argument, which is an object
    that responds to two methods, add and incr.  The cache object is used to
    manage the value for last_updated.  ``add`` is called on initialize to
    create the key if it does not exist with the default value, and ``incr`` is
    done to atomically update the last_updated value.
    """

    def __init__(self, manager, cache, key_col='key', value_col='value', *args, **kwargs):
        self.manager = manager
        self.cache = cache
        self.cache_key = 'last_updated'
        self.key_col = key_col
        self.value_col = value_col
        self.cache.add(self.cache_key, 1)  # Only adds if key does not exist
        super(ModelDict, self).__init__(*args, **kwargs)

    def persist(self, key, val):
        instance, created = self.get_or_create(key, val)

        if not created and getattr(instance, self.value_col) != val:
            setattr(instance, self.value_col, self._encode(val))
            instance.save()

        self.__touch_last_updated()

    def depersist(self, key):
        self.manager.get(**{self.key_col: key}).delete()
        self.__touch_last_updated()

    def persistents(self):
        encoded_tuples = self.manager.values_list(self.key_col, self.value_col)
        tuples = [(k, self._decode(v)) for k, v in encoded_tuples]
        return dict(tuples)

    def _setdefault(self, key, default=None):
        instance, created = self.get_or_create(key, default)

        if created:
            self.__touch_last_updated()

        return self._decode(getattr(instance, self.value_col))

    def _pop(self, key, default=None):
        try:
            instance = self.manager.get(**{self.key_col: key})
            value = self._decode(getattr(instance, self.value_col))
            instance.delete()
            self.__touch_last_updated()
            return value
        except self.manager.model.DoesNotExist:
            if default is not None:
                return default
            else:
                raise KeyError

    def get_or_create(self, key, val):
        return self.manager.get_or_create(
            defaults={self.value_col: self._encode(val)},
            **{self.key_col: key}
        )

    def last_updated(self):
        return self.cache.get(self.cache_key)

    def __touch_last_updated(self):
        self.cache.incr('last_updated')


class MemoryDict(PersistedDict):
    '''
    Does not actually persist any data to a persistant storage.  Instead, keeps
    everything in memory.  This is really only useful for use in tests
    '''

    def __init__(self, *args, **kwargs):
        self.__storage = dict()
        self.__last_updated = 1
        super(MemoryDict, self).__init__(*args, **kwargs)

    def persist(self, key, val):
        self.__storage[key] = self._encode(val)
        self.__last_updated += 1

    def depersist(self, key):
        del self.__storage[key]
        self.__last_updated += 1

    def persistents(self):
        encoded_tuples = self.__storage.items()
        tuples = [(k, self._decode(v)) for k, v in encoded_tuples]
        return dict(tuples)

    def last_updated(self):
        return self.__last_updated

    def _setdefault(self, key, default=None):
        self.__last_updated += 1
        val = self.__storage.setdefault(key, self._encode(default))
        return self._decode(val)

    def _pop(self, key, default=None):
        self.__last_updated += 1

        if default:
            default = self._encode(default)

        val = self.__storage.pop(key, default)

        if val is None:
            raise KeyError

        return self._decode(val)

    def _encode(self, value):
        return value

    def _decode(self, value):
        return value


def validate_key(func):
    """
    Decorator to validate a key for zookeeper.
    """

    @wraps(func)
    def wrapper(self, key, *args, **kwargs):
        if posixpath.sep in key:
            raise ValueError('Keys cannot contains slashes')

        return func(self, key, *args, **kwargs)

    return wrapper


class ZookeeperDict(PersistedDict):
    """
    Dictionary backed by Zookeeper.  Functions just as you would expect a normal
    dictiony to function, except the values in the dictionary are persisted and
    loaed from Zookeper located at the spcified ``path`` in the constructor.

    Due to Zookeeper's watchers, this dictionary has the nice property that the
    called to its ``last_updated`` method (to check if the storage has been
    updated since the dict was last synced) simply returns a cached value -- it
    does not query Zookeeper or anything like that, so it's basically free.
    This means that you can run this dictionary with ``autosync=False`` and it
    will still be reasonably performant.

    Dictionary keys in a ``ZookeeperDict`` are stored at inividual nodes in the
    zookeeper heirarchy, with the value of the node being the value of that key.
    Each node for each dict key is a child of the "root" node, whose path is
    specified with the ``path`` argument in the constructor.

        >>> from modeldict.dict import ZookeeperDict
        >>> from kazoo.client import KazooClient
        >>> kazoo = KazooClient()
        >>> kazoo.start()
        >>> zkdict = ZookeeperDict(kazoo, '/app/config')
        >>> zkdict['exchange_rate'] = 25
        >>> zkdict['language'] = 'en-US'
        >>> zkdict['exchange_rate']
        25
        >>> zkdict['language']
        'en-US'
        >>> zkdict.pop('exchange_rate')
        25
        >>> zkdict['exchange_rate']
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "modeldict/base.py", line 53, in __getitem__
            return self.__dict.__getitem__(key)
        KeyError: 'exchange_rate'

    NOTE: unlike ``RedisDict`` or ``ModelDict``, which are backed by hightly
    consistent backend storages, ``ZookeeperDict`` is backed with Zookeeper,
    which has looser consistency guarantees.

    Please see this page in the Zookeeper docs for details:

    http://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#ch_zkGuarantees

    The basic things to keep in mind are:

        Sequential Consistency:
        Updates from a client will be applied in the order that they were sent.

        Atomicity:
        Updates either succeed or fail -- there are no partial results.

        Single System Image:
        A client will see the same view of the service regardless of the server
        that it connects to.

        Reliability:
        Once an update has been applied, it will persist from that time forward
        until a client overwrites the update. This guarantee has two
        corollaries:

            If a client gets a successful return code, the update will have been
            applied. On some failures (communication errors, timeouts, etc) the
            client will not know if the update has applied or not. We take steps
            to minimize the failures, but the only guarantee is only present
            with successful return codes. (This is called the monotonicity
            condition in Paxos.)

            Any updates that are seen by the client, through a read request or
            successful update, will never be rolled back when recovering from
            server failures.

        Timeliness:
        The clients view of the system is guaranteed to be up-to-date within a
        certain time bound. (On the order of tens of seconds.) Either system
        changes will be seen by a client within this bound, or the client will
        detect a service outage.

    The cliff notes version of this is that a client's view of the world will
    always be consistent (you can read your own writes), but updates from other
    clients can take time to propogate to other clients.
    """

    def __init__(self, zk, path, *args, **kwargs):
        """
        Construct a new instance of a ``ZookeeperDict``.

        :param zk: Zookeeper client, likely ``KazooClient``
        :type zk: KazooClient
        :param path: The path to the root config node.
        :type path: string
        :param autosync: Sync with Zookeeper before each read.
        :type autosync: bool
        """
        self.zk = zk
        self.path = path
        self.zk.ensure_path(self.path)
        self._last_updated = None

        # TODO: The base PersistedDict class updates last_updated itself
        # manually when adding a new key with __setattr__, as well as this watch
        # also incrementing the value.
        self.child_watch = self.zk.ChildrenWatch(
            self.path,
            self.__increment_last_updated
        )

        super(ZookeeperDict, self).__init__(*args, **kwargs)

    def last_updated(self):
        """
        Ever-increasing integer, which is bumped any time a key in Zookeeper has
        been changed (created, updated, deleted).

        The value in incremented manually by an instances when updating the
        dict, as well as when other instances of the dict update persistant
        storage, via a Zookeeper watch on the root config node.
        """
        return self._last_updated

    @validate_key
    def persist(self, key, value):
        """
        Encode and save ``value`` at ``key``.

        :param key: Key to store ``value`` at in Zookeeper.
        :type key: string
        :param value: Value to store. Encoded before being stored.
        :type value: value
        """
        encoded = self._encode(value)
        self.__set_or_create(key, encoded)
        self.__increment_last_updated()

    @validate_key
    def depersist(self, key):
        """
        Remove ``key`` from dictionary.

        :param key: Key to remove from Zookeeper.
        :type key: string
        """
        self.zk.delete(self.__path_of(key))
        self.__increment_last_updated()

    def persistents(self):
        """
        Dictionary of all keys and their values in Zookeeper.
        """
        results = dict()

        for child in self.zk.get_children(self.path):
            value, _ = self.zk.get(
                self.__path_of(child),
                watch=self.__increment_last_updated
            )
            results[child] = self._decode(value)

        return results

    @validate_key
    def _pop(self, key, default=None):
        """
        If ``key`` is present in Zookeeper, removes it from Zookeeper and
        returns the value.  If key is not in Zookeper and ``default`` argument
        is provided, ``default`` is returned.  If ``default`` argument is not
        provided, ``KeyError`` is raised.

        :param key: Key to remove from Zookeeper
        :type key: string
        :param default: Default object to return if ``key`` is not present.
        :type default: object
        """
        path = self.__path_of(key)
        value = None

        try:
            # We need to both delete and return the value that was in ZK here.
            raw_value, _ = self.zk.get(path)
            value = self._decode(raw_value)
        except NoNodeError:
            # The node is already gone, so if a default is given, return it,
            # otherwise, raise KeyError
            if default:
                return default
            else:
                raise KeyError

        # Made it this far, it means have a value from the node and it existed
        # at least by that point in time
        try:
            # Try to delete the node
            self.zk.delete(path)
            self.__increment_last_updated()
        except NoNodeError:
            # Someone deleted the node in the mean time...how nice!
            pass

        return value

    @validate_key
    def _setdefault(self, key, default=None):
        """
        If ``key`` is not present, set it as ``default`` and return it.  If
        ``key`` is present, return its value.

        :param key: Key to add to Zookeeper
        :type key: string
        :param default: Default object to return if ``key`` is present.
        :type default: object

        Will retry trying to get or create a node based on the "retry" config
        from the Kazoo client.
        """
        return self.zk.retry(self.__inner_set_default, key, default)

    def __path_of(self, key):
        return posixpath.join(self.path, key)

    def __set_or_create(self, key, value):
        path = self.__path_of(key)
        self.zk.ensure_path(path)
        self.zk.set(path, value)

    def __increment_last_updated(self, children=None):
        if self._last_updated is None:
            self._last_updated = 0

        self._last_updated += 1

    def __inner_set_default(self, key, value):
        """
        Tries to return the value at key.  If the key does not exist, attempts
        to create it with the value.  If the node is created in the mean time,
        a ``NodeExistsError`` will be raised.
        """
        path = self.__path_of(key)

        try:
            # Try to get and return the existing node with its data
            value, _ = self.zk.get(path)
            return self._decode(value)
        except NoNodeError:
            # Node does not exist, we have to create it
            self.zk.create(path, self._encode(value))
            self.__increment_last_updated()
            return value
