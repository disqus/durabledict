from base import PersistedDict


class ModelDict(PersistedDict):
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
            setattr(instance, self.value_col, self._encode(val))
            instance.save()

        self.__touch_last_updated()

    def depersist(self, key):
        self.manager.get(**{self.key_col: key}).delete()
        self.__touch_last_updated()

    def persistents(self):
        if self.return_instances:
            return dict((i.key, i) for i in self.manager.all())
        else:
            encoded_tuples = self.manager.values_list(
                self.key_col,
                self.value_col
            )
            return dict((k, self._decode(v)) for k, v in encoded_tuples)

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
