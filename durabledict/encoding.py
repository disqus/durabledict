import pickle
import base64


try:
    import simplejson as json
except ImportError:
    import json


class NoOpEncoding(object):
    encode = staticmethod(lambda d: d)
    decode = staticmethod(lambda d: d)


class PickleEncoding(object):

    @staticmethod
    def encode(data):
        pickled = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
        return base64.encodestring(pickled)

    @staticmethod
    def decode(data):
        pickled = base64.decodestring(data)
        return pickle.loads(pickled)


class JSONEncoding(object):
    encode = staticmethod(json.dumps)
    decode = staticmethod(json.loads)
