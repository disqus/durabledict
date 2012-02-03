----------------
Modeldict
----------------

Modeldict contains a collection of dictionary classes backed by a persistent data store (Redis, Django models, etc) suitable for use in a distributed manner.  Dictionary values are cached locally in the instance of the dictionary, and only sync with their values with their persistent data stores when a value in the data store has changed.

Usage
-----

Modeldict contains various flavors of a dictionary-like objects backed by a persistent data store.  All dicts classes are located in the ``modeldict.dict`` namespace.  At present, Modeldict offers the following dicts:

1. ``modeldic.dict.RedisDict`` - Redis-backed persistent storage
2. ``modeldic.dict.ModelDict`` - DB object (most likely Django model)-backed persistent storage.

Each different dictionary class has a different ``__init__`` method which take different arguments, so consult their documentation for specific usage detail.

Once you have an instance of a modeldict, just use it like you would a normal dictionary.::

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
        >>> KeyError

Notes on Persistence, Consistency and the In-Memory Cache
-----------------------------

Nearly all methods called on a ``modeldict.dict`` dictionary class (i.e. ``RedisDict``) are proxied to an internal dict object that serves as a cache for access to dict values.  This cache is only updated with values freshly from persistent storage if there actually has been a change in the values stored in persistent storage

To check if the data in persistent storage has changed, each modeldict.dict backend is responsible for providing a fast ``last_updated`` method that quickly tells the dict the last time any value in the storage has been updated.  For instance, the ``ModelDict`` constructor requires a ``cache`` object passed in as an argument, which provides implementations of cache-line interface methods for maintaining the ``last_updated`` state.  A memcache client is a good candidate for this object.

Out of the box by default, all ``modeldict.dict`` classes will sync with their persistent data store on all writes (insert, updates and deletes) as well as immediately before any read operation on the dictionary.  This mode provides *high read consistency* of data at the expense of read speed.  You can be guaranteed that any read operation on your dict, i.e. ``settings['cool_feature']``, will always use the most up to data.  If another consumer of your persistent data store has modified a value in that store since you instantiated your object, you will immediately be able to read the new data with your dict instance.

Manually Control Persistent Storage Sync
------------------------------------------

As mentioned above in "Notes on Persistence, Consistency and the In-Memory Cache,"" the downside to syncing with persistent storage before each read of dict data is it lowers your performance.  If you read 100 keys from your dictionary, that means 100 accesses to check ```last_updated``.  Even with a data store as fast as memecache, that adds up quite quickly.

It therefore may be advantageous for you to not sync with persistent storage before every dead from the dict and instead control that manually.  To do so, pass ``autosync=False`` when you construct the class, i.e.::

        from modeldict.dict import RedisDict
        from redis import Redis

        # Construct a new RedisDict object that does not sync on reads
        settings = RedisDict('settings', Redis(), autosync=False)

This causes the dictionary behave in the following way:

1. Like normal, the dictionary initializes from the persistent data store upon instantiation.
2. Writes (both inserts and updates), along with deletes of values to the dictionary will still automatically sync with the data store each time the operation happens.
3. Any time a dictionary is read from, only data current in the internal cache is used.  The dict *will not attempt to sync with its persistent data store* before reads.
4. To force the dict to attempt to sync with its persistent data store, you may call the ``sync()`` method on the dictionary.  As with when ``autosync`` is false, if ``last_update`` says there are no changes, the dict will still not fetch and update itsself with values from persistent storage.

A good use case for manual syncing is a read-heavy web application, where you're using a modeldict for settings configuration.  Very few requests actually *change* the dictionary contents - most simply read from the dictionary.  In this situation, you would perhaps only ``sync()`` at the beginning of a user's web request to make sure the dict is up to date, but then not during the request in order to push the response to the user as fast as possible.

Integration with Django
------------------------

If you would like to store your dict values in the dadatabase for your Django application, you should use the ``modeldict.dict.modelDict`` class.  This class takes an instance of a model's manager, as well as ``key_col`` and ``value_col`` arguments which can be used to tell ModelDict which columns on your object it should use to store data.

It's also probably most adventageuous to construct your dicts with ``autosync=False`` (see "Manually Control Persistent Storage Sync" above) and manually call ``sync()`` before each request.  This can be acomlished most easily via the ``request_started`` signal::

        django.core.signals.request_started.connect(settings.sync)

Possible Future Additions
------------------------

These are features that may be added to ModelDict at some point in the future.

1. Support ``auto_create`` in ``ModelDict``
2. Support ``instances`` vs values in ``ModelDict``