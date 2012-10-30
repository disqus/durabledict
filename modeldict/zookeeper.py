from modeldict.base import PersistedDict

from kazoo.exceptions import NoNodeError
from functools import wraps
import posixpath


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
