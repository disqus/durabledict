from modeldict.base import PersistedDict
import time


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

    def persist(self, key, val):
        self.__touch_last_update_and('hset', self.keyspace, key, val)

    def depersist(self, key):
        self.__touch_last_update_and('hdel', self.keyspace, key)

    def persistants(self):
        return self.conn.hgetall(self.keyspace)

    def last_updated(self):
        return int(self.conn.get(self.__last_update_key) or 0)

    def __touch_last_updated(self):
        return self.conn.incr(self.__last_update_key)

    def __touch_last_update_and(self, method, *args, **kwargs):
        with self.conn.pipeline() as pipe:
            getattr(pipe, method)(*args, **kwargs)
            pipe.incr(self.__last_update_key)
            pipe.execute()

    @property
    def __last_update_key(self):
        return self.keyspace + 'last_updated'


class ModelDict(PersistedDict):
    """
    Dictionary-style access to a model. Populates a cache and a local in-memory
    to avoid multiple hits to the database.

    Specifying ``instances=True`` will cause the cache to store instances rather
    than simple values.

    If ``auto_create=True`` accessing modeldict[key] when key does not exist will
    attempt to create it in the database.

    Functions in two different ways, depending on the constructor:

        # Given ``Model`` that has a column named ``foo`` where the value at
        # that column is "bar":

        mydict = ModelDict(Model, value_col='foo')
        mydict['test']
        >>> 'bar' #doctest: +SKIP

    If you want to use another key in the ModelDict besides the ``Model``s
    ``pk``, you may specify that in the constructor with ``key_col``.  For
    instance, if your ``Model`` has a column called ``id``, you can index into
    that column by passing ``key_col='id'`` in to the contructor:

        mydict = ModelDict(Model, key_col='id', value_col='foo')
        mydict['test']
        >>> 'bar' #doctest: +SKIP

    The constructor also takes a cache keyword argument, which is an object that
    responds to two methods, add and incr.  The cache object is used to manage
    the value for last_updated.  ``add`` is called on initialize to create the
    key if it does not exist with the default value, and ``incr`` is done to
    atomically update the last_updated value.

    """

    def __init__(self, manager, cache, key_col='key', value_col='value'):
        self.manager = manager
        self.cache = cache
        self.cache_key = 'last_updated'
        self.key_col = key_col
        self.value_col = value_col
        self.cache.add(self.cache_key, 1) # Only adds if key does not exist
        super(ModelDict, self).__init__()


    def persist(self, key, val):
        instance, created = self.manager.get_or_create(
            defaults={self.value_col: val},
            **{self.key_col: key}
        )

        if not created and getattr(instance, self.value_col) != val:
            setattr(instance, self.value_col, val)
            instance.save()

        self.__touch_last_updated()

    def depersist(self, key):
        self.manager.get(**{self.key_col: key}).delete()
        self.__touch_last_updated()

    def persistants(self):
        return dict(
            self.manager.values_list(self.key_col, self.value_col)
        )

    def last_updated(self):
        return self.cache.get(self.cache_key)

    def __touch_last_updated(self):
        self.cache.incr('last_updated')

    # def __init__(self, model, key='pk', value=None, instances=False, auto_create=False, *args, **kwargs):
    #     assert value is not None

    #     super(ModelDict, self).__init__(*args, **kwargs)

    #     self.key = key
    #     self.value = value

    #     self.model = model
    #     self.instances = instances
    #     self.auto_create = auto_create

    #     self.cache_key = 'ModelDict:%s:%s' % (model.__name__, self.key)
    #     self.last_updated_cache_key = 'ModelDict.last_updated:%s:%s' % (model.__name__, self.key)

    # def __setitem__(self, key, value):
    #     if isinstance(value, self.model):
    #         value = getattr(value, self.value)
    #     instance, created = self.model._default_manager.get_or_create(
    #         defaults={self.value: value},
    #         **{self.key: key}
    #     )

    #     # Ensure we're updating the value in the database if it changes, and
    #     # if it was frehsly created, we need to ensure we populate our cache.
    #     if getattr(instance, self.value) != value:
    #         # post_save hook hits so we dont need to populate
    #         setattr(instance, self.value, value)
    #         instance.save()
    #     elif created:
    #         self._populate(reset=True)

    # def __delitem__(self, key):
    #     self.model._default_manager.filter(**{self.key: key}).delete()

    # def setdefault(self, key, value):
    #     if isinstance(value, self.model):
    #         value = getattr(value, self.value)
    #     instance, created = self.model._default_manager.get_or_create(
    #         defaults={self.value: value},
    #         **{self.key: key}
    #     )
    #     self._populate(reset=True)

    # def get_default(self, value):
    #     if not self.auto_create:
    #         return NoValue
    #     return self.model.objects.create(**{self.key: value})

    # def _get_cache_data(self):
    #     qs = self.model._default_manager
    #     if self.instances:
    #         return dict((getattr(i, self.key), i) for i in qs.all())
    #     return dict(qs.values_list(self.key, self.value))

    # # Signals

    # def _post_save(self, sender, instance, created, **kwargs):
    #     if self._cache is None:
    #         self._populate()
    #     if self.instances:
    #         value = instance
    #     else:
    #         value = getattr(instance, self.value)
    #     key = getattr(instance, self.key)
    #     if value != self._cache.get(key):
    #         self._cache[key] = value
    #     self._populate(reset=True)

    # def _post_delete(self, sender, instance, **kwargs):
    #     if self._cache:
    #         self._cache.pop(getattr(instance, self.key), None)
    #     self._populate(reset=True)
