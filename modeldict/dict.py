from modeldict.base import PersistedDict


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

    def __contains__(self, key):
        return self.conn.hexists(self.keyspace, key)


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
