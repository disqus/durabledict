from durabledict.encoding import PickleEncoding


class DurableDict(object):
    """
    Dictionary that calls out to its persistant data store when items are
    created or deleted.  Syncs with data fron the data store before every read,
    unless ``autosync=False`` is passed, which causes the dict to only sync
    data from the data store on writes and when ``sync()`` is called.

    By default, objects are encoded in the durable store using
    ``encoding.PickleEncoding``, but that can be changed by passing in another
    encoder in as the ``encoding`` kwarg.
    """

    def __init__(self, autosync=True, encoding=PickleEncoding):
        self.__dict = dict()
        self.last_synced = 0
        self.autosync = autosync
        self.encoding = encoding
        self.__sync_with_durable_storage(force=True)

    @property
    def cache_expired(self):
        persistance_last_updated = self.last_updated()

        if not self.last_synced or persistance_last_updated > self.last_synced:
            return persistance_last_updated

    def sync(self):
        self.__sync_with_durable_storage(force=True)

    def pop(self, key, default=None):
        result = self._pop(key, default)
        self.__sync_with_durable_storage(force=True)
        return result

    def setdefault(self, key, default=None):
        result = self._setdefault(key, default)
        self.__sync_with_durable_storage(force=True)
        return result

    def get(self, key, default=None):
        self.__sync_with_durable_storage()
        return self.__dict.get(key, default)

    def __setitem__(self, key, val):
        self.persist(key, val)
        self.__sync_with_durable_storage(force=True)

    def __delitem__(self, key):
        self.depersist(key)
        self.__sync_with_durable_storage(force=True)

    def __getitem__(self, key):
        self.__sync_with_durable_storage()
        return self.__dict.__getitem__(key)

    def __getattr__(self, name):
        self.__sync_with_durable_storage()
        return getattr(self.__dict, name)

    def __len__(self):
        self.__sync_with_durable_storage()
        return self.__dict.__len__()

    def __cmp__(self, other):
        self.__sync_with_durable_storage()
        return self.__dict.__cmp__(other)

    def __repr__(self):
        return self.__dict.__repr__()

    def __contains__(self, key):
        self.__sync_with_durable_storage()
        return self.__dict.__contains__(key)

    def __sync_with_durable_storage(self, force=False):
        if not self.autosync and not force:
            return

        cache_expired_at = self.cache_expired

        if cache_expired_at:
            self.__dict = self.durables()
            self.last_synced = cache_expired_at

    def persist(self, key, val):
        raise NotImplementedError

    def depersist(self, key):
        raise NotImplementedError

    def durables(self):
        raise NotImplementedError

    def last_updated(self):
        raise NotImplementedError
