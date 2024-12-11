
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm


def serializeToBundle(self, path, dataset=None):
    serializer = SimpleSparkSerializer()
    serializer.serializeToBundle(self, path, dataset=dataset)


def deserializeFromBundle(path):
    serializer = SimpleSparkSerializer()
    return serializer.deserializeFromBundle(path)

setattr(Transformer, 'serializeToBundle', serializeToBundle)
setattr(Transformer, 'deserializeFromBundle', staticmethod(deserializeFromBundle))


class SimpleSparkSerializer(object):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()
        self._java_obj = _jvm().ml.combust.mleap.spark.SimpleSparkSerializer()

    def serializeToBundle(self, transformer, path, dataset):
        self._java_obj.serializeToBundle(transformer._to_java(), path, dataset._jdf)

    def deserializeFromBundle(self, path):
        return JavaTransformer._from_java(self._java_obj.deserializeFromBundle(path))