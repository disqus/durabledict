from base import ConnectionDurableDict


class RedisDict(ConnectionDurableDict):

    """
    Dictionary-style access to a redis hash table. Populates a cache and a local
    in-memory to avoid multiple hits to the database.

    Functions just like you'd expect it::

        mydict = RedisDict('my_redis_key', Redis())
        mydict['test']
        >>> 'bar' #doctest: +SKIP
    """

    def __init__(self, **kwargs):
        super(RedisDict, self).__init__(**kwargs)
        self.__touch_last_updated()

    def persist(self, key, value):
        encoded = self.encoding.encode(value)
        self.__touch_and_multi(('hset', (self.keyspace, key, encoded)))

    def depersist(self, key):
        self.__touch_and_multi(('hdel', (self.keyspace, key)))

    def durables(self):
        encoded = self.connection.hgetall(self.keyspace)
        tuples = [(k, self.encoding.decode(v)) for k, v in encoded.items()]
        return dict(tuples)

    def last_updated(self):
        return int(self.connection.get(self.__last_update_key) or 0)

    # TODO: setdefault always touches the last_updated value, even if the key
    # existed already.  It should only touch last_updated if the key did not
    # already exist
    def _setdefault(self, key, default=None):
        encoded = self.__touch_and_multi(
            ('hsetnx', (self.keyspace, key, self.encoding.encode(default))),
            ('hget', (self.keyspace, key)),
            returns=-1
        )
        return self.encoding.decode(encoded)

    def _pop(self, key, default=None):
        last_updated, encoded, key_existed = self.__touch_and_multi(
            ('hget', (self.keyspace, key)),
            ('hdel', (self.keyspace, key))
        )

        if key_existed:
            return self.encoding.decode(encoded)
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

        with self.connection.pipeline() as pipe:
            pipe.incr(self.__last_update_key)
            [getattr(pipe, function)(*a) for function, a in args]
            results = pipe.execute()

            if kwargs.get('returns'):
                return results[kwargs.get('returns')]
            else:
                return results

    def __touch_last_updated(self):
        return self.connection.incr(self.__last_update_key)

    @property
    def __last_update_key(self):
        return self.keyspace + 'last_updated'
