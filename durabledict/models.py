from base import DurableDict


class ModelDict(DurableDict):
    """
    Dictionary-style access to a model. Populates a cache and a local in-memory
    to avoid multiple hits to the database.

        # Given ``Model`` that has a primary key (``pk``) column that can be
        # strings:

        mydict = ModelDict(Model.manager, value_col='foo')
        mydict['test']
        >>> 'bar' #doctest: +SKIP

    The first positional argument to ``ModelDict`` is ``manager``, which is an
    instance of a Manager which ``ModelDict`` uses to read and write to your
    database.  Any object that conforms to the interface can work, but the
    expectation is that ``manager`` is a Django.model manager.

    The constructor also takes a 2nd positional ``cache`` argument, which is an
    object that responds to two methods, add and incr.  The cache object is used
    to manage the value for last_updated.  ``add`` is called on initialize to
    create the key if it does not exist with the default value, and ``incr`` is
    done to atomically update the last_updated value.

    By default, ``ModelDict`` will use the ``pk`` column of your model as the
    "key" for the dictionary.  If you want to use another key besides the
    ``Model``s ``pk``, you may specify that in the constructor with ``key_col``
    kwarg.  For instance, if your ``Model`` has a column called ``id``, you can
    index into that column by passing ``key_col='id'`` in to the contructor:

        mydict = ModelDict(Model, key_col='id', value_col='foo')
        mydict['test']
        >>> 'bar' #doctest: +SKIP

    Additionally, the default column used to store the values is the ``value``
    column.  If you'd like to use another column pass the ``value_col`` kwarg to
    the constructor.

    By default, ``ModelDict`` instances will return the decoded value present in
    the ``value_col``.  If, instead, you would like to return the entire model
    instance, you can instead pass ``True`` to the ``return_instances`` kwarg.
    """

    #: The value to increate last_updated by, in the event that the key is
    # missing from the cache.  See the comment inside touch_last_updated for
    # details.
    LAST_UPDATED_MISSING_INCREMENT = 1000

    def __init__(self, manager, cache, *args, **kwargs):
        self.manager = manager
        self.cache = cache
        self.cache_key = 'last_updated'
        self.return_instances = kwargs.pop('return_instances', False)

        self.key_col = kwargs.pop('key_col', 'key')
        self.value_col = kwargs.pop('value_col', 'value')

        self.cache.add(self.cache_key, 1)  # Only adds if key does not exist

        super(ModelDict, self).__init__(*args, **kwargs)

    def persist(self, key, val):
        instance, created = self.get_or_create(key, val)

        if not created and getattr(instance, self.value_col) != val:
            setattr(instance, self.value_col, self.encoding.encode(val))
            instance.save()

        self.touch_last_updated()

    def depersist(self, key):
        self.manager.get(**{self.key_col: key}).delete()
        self.touch_last_updated()

    def durables(self):
        if self.return_instances:
            return dict((i.key, i) for i in self.manager.all())
        else:
            encoded_tuples = self.manager.values_list(
                self.key_col,
                self.value_col
            )
            return dict((k, self.encoding.decode(v)) for k, v in encoded_tuples)

    def _setdefault(self, key, default=None):
        instance, created = self.get_or_create(key, default)

        if created:
            self.touch_last_updated()

        return self.encoding.decode(getattr(instance, self.value_col))

    def _pop(self, key, default=None):
        try:
            instance = self.manager.get(**{self.key_col: key})
            value = self.encoding.decode(getattr(instance, self.value_col))
            instance.delete()
            self.touch_last_updated()
            return value
        except self.manager.model.DoesNotExist:
            if default is not None:
                return default
            else:
                raise KeyError

    def get_or_create(self, key, val):
        return self.manager.get_or_create(
            defaults={self.value_col: self.encoding.encode(val)},
            **{self.key_col: key}
        )

    def last_updated(self):
        return self.cache.get(self.cache_key)

    def touch_last_updated(self):
        try:
            self.cache.incr(self.cache_key)
        except ValueError:
            # The last_updated cache key is missing. This may be because it has
            # expired or been explicitly deleted. It is then necessary to
            # recreate the value. by synthesizing an ``incr``. This is
            # accomplished by adding the current ``self.last_synced`` value plus
            # some increment, naively "1".
            #
            # However, there is a race condition. It is entirely possible that
            # an instance of DurableDict which sees the key as missing may
            # have an out of date value for ``self.last_synced`` - such as if
            # multiple updates have occurred from other instances of DurableDict
            # (and are reflected in the cache value of last_udated), but this
            # instance of DurableDict has not seen those changes yet. If the
            # cache key is deleted before this instance hasn't seen those
            # changed, but this instance sees the cache as expired, then it will
            # update the value to what it knows to be last_synced + 1, which is
            # incorrect.
            #
            # A workaround to this problem is to set the new cache value to what
            # this instance thinks last_synced is + some non-trivial number.
            # This is not a perfect solution, as it's possible there could be
            # LAST_UPDATED_MISSING_INCREMENT changes to the dict since this
            # instance last checked, so LAST_UPDATED_MISSING_INCREMENT should
            # be set to a value high enough to where that possibility is
            # unlikely.
            added_key = self.cache.add(
                self.cache_key,
                self.last_synced + self.LAST_UPDATED_MISSING_INCREMENT
            )

            # XXX There is still  race condition here. It is possible that the
            # key can be deleted between the call to ``add`` and ``incr``, in
            # which case the ``incr`` call will fail.  This is a unlikely
            # scenario and as such is not handled.

            # If the ``add`` did not succeed, it means that another instance of
            # DurableDict beat this one to adding the cache value back and so
            # this instance should do the ``incr`` like normal.
            if not added_key:
                self.cache.incr(self.cache_key)
