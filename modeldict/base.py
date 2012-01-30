NoValue = object()


class PersistedDict(dict):
    """
    Dictionary that calls out to its persistant data store when items are
    created or deleted.
    """

    def __init__(self):
        self._cache_stale = None
        self._last_updated = None

    def __setitem__(self, key, val):
        self._persist(key, val)
        return super(PersistedDict, self).__setitem__(key, val)

    def __delitem__(self, key):
        self._depersist(key)
        return super(PersistedDict, self).__delitem__(key)

    def _persist(self, key, val):
        pass

    def _depersist(self, key):
        pass
