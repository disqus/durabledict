----------------
Modeldict
----------------

Modeldict contains a collection of dictionary classes backed by a persistent data store (Redis, Django models, etc) suitable for use in a distributed manner.  Dictionary values are cached locally in the instance of the dictionary, but sync with their persistent data stores when a value in the data store has changed.

TODO
----

1. Support ``auto_create`` in ``ModelDict``
2. Support ``instances`` vs values in ``ModelDict``
3. Look at hooking into the request/response cycle in Django.
4. Add examples back.