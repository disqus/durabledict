----------------
Modeldict
----------------

Modeldict contains a collection of dictionary classes backed by a persistent data store (Redis, Django models, etc) suitable for use in a distributed manner.  Dictionary values are cached locally in the instance of the dictionary, but sync with their persistent data stores when a value in the data store has changed.

Usage
-----

Modeldict contains various flavors of a dictionary-like object backed by a persistent data store.  All dicts classes are located in the modeldict.dict namespace.  At present, Modeldict offers the following dicts:

1. ``RedisDict`` - Redis-backed persistent storage
2. ``ModelDict`` - DB object (most likely Django model-backed) persistent storage.

Each different dict.class has a different ``__init`` method which take different arguments, so consult their documentation for specific usage detail.

Once you have an instance of a modeldict, just use it like you would a normal dictionary.

    from modeldict.dict import RedisDict
    from redis import Redis

    # Construct a new RedisDict object
    settings = RedisDict('settings', Redis())

    # Assign and retrieve a value from the dict
    settings['foo'] = 'bar'
    settings['foo']
    >>> 'bar'

    # Assign and retrieve another value
    settings['buzz'] = 'foogle'
    settings['buzz']
    >>> 'foogle'

    # Delete a value and access receives a KeyError
    del settings['foo']
    settings['foo']
    KeyError

Notes on Persistance, Consistency and the In-Memory Cache
-----------------------------

Nearly all methods called on a ``modeldict.dict`` dictionary class (i.e. ``RedisDict``) are proxied to an internal dict object that serves as a cache for access to dict keys.  This cache is only invalidated (with values freshly fetched from persistent storage again) if there actually has been a change in the persistent storage.  Each modeldict.dict backend is responsible for providing a fast "has anything changed?" method that quickly tells the dict if anything has changed.  For instance, the ``ModelDict`` constructor requires a ``cache`` argument that provides a certain "cache-like" interface for maintaining the "has anything changed?" state.  Memcache is a good candidate for this cache.

Out of the box by default, all ``modeldict.dict`` classes will sync with their persistent data store on all writes (insert, updates and deletes) as well as immediately before any read operation on the dictionary.  This mode provides *high read consistency* of data at the expense of speed.  You can be guaranteed that any read operation on your dict, i.e. ``settings['cool_feature']``, will always use the most up to data.  If another consumer of your persistent data store has modified a value since you instantiated your object, you will see that change any time you use the dictionary.

Manual Control Syncs to Persistent Storage
------------------------------------------

As mentioned above in "Notes on Persistence, Consistency and the In-Memory Cache,"" the downside to syncing with persistent storage before each read of dict data is it's time consuming.  If you read 100 keys from your dictionary, that means 100 accesses to check "has anything changed yet?"  Even with memecache, that adds up.

To manually control syncs with the dict's persistent data storage, pass ``autosync=False`` when you construct the class, i.e.:

    from modeldict.dict import RedisDict
    from redis import Redis

    # Construct a new RedisDict object
    settings = RedisDict('settings', Redis(), autosync=False)

This causes the dictionary behave in the following way:

1. Writes (both inserts and updates), along with deletes of values to the dictionary will still automatically sync with the data store.
2. Any time a dictionary is read from, the internal cache will only be used and the dict *will not sync with its persistent data store*.
3. To force the dict to sync with its persistent data store, you may call the ``sync()`` method on the dictionary.

A good use case for manual syncing is a read-heavy web application, where you're using a modeldict for settings configuration.  Very few requests actually *change* the dictionary contents - most simply read from the dictionary.  In this situation, you would perhaps only ``sync()`` at the beginning of a user's web request to make sure the dict is up to date, but then not during the request in order to push the response to the user as fast as possible.

Possible Future Additions
------------------------

These are features that may be added to ModelDict at some point in the future.

1. Support ``auto_create`` in ``ModelDict`` 2. Support ``instances`` vs values
in ``ModelDict``
