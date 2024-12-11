import os

from pyspark.ml.util import _jvm
from pyspark.ml.wrapper import JavaWrapper

from pyspark.sql import DataFrame

from oo4oo.pyspark.py2scala import jvm_scala_object

class PipelineDAG(JavaWrapper):
    def __init__(self, java_dag):
        #super(PipelineDAGHelper, self).__init__()
        self._java_obj = java_dag
    def fit(self, input: dict[str, DataFrame], verbose: int=0):
        java_input = _jvm().PythonUtils.toScalaMap({k:v._jdf for k,v in input.items()})
        return self._java_obj.fit(java_input, verbose)
    def transform(self, input: dict[str, DataFrame], verbose: int=0):
        java_input = _jvm().PythonUtils.toScalaMap({k:v._jdf for k,v in input.items()})
        java_output = self._java_obj.sparkTransform(java_input, verbose)

        output = DataFrame(java_output, list(input.values())[0].sparkSession)
        return output
    def save(self, basePath):
        self._java_obj.save(basePath)

class PipelineDAGHelper(JavaWrapper):
    def __init__(self):
        #super(PipelineDAGHelper, self).__init__()
        self._java_obj = jvm_scala_object(_jvm().ml.oo4oo.dag, f"PipelineDAGHelper$")

    def apply(self, stagesCtx):
        obj = self._java_obj.apply(stagesCtx)
        return PipelineDAG(obj)

    def load(self, basePath):
        obj = self._java_obj.load(basePath)
        return PipelineDAG(obj)

    def placeHolder(self, input):
        return self._java_obj.placeHolder(input)

    def select(self, stagesCtx, cols: list[str]):
        java_cols = _jvm().PythonUtils.toSeq(cols)
        return self._java_obj.select(stagesCtx, java_cols)

    def toNode(self, stagesCtx):
        return self._java_obj.toNode(stagesCtx)

    def join(self, left, right, usingColumns, joinType):
        java_cols = _jvm().PythonUtils.toSeq(usingColumns)
        return self._java_obj.join(left, right, java_cols, joinType)

    def withColumn(self, stagesCtx, output: str, op: str, inputs: list[str]):
        java_cols = _jvm().PythonUtils.toSeq(inputs)
        return self._java_obj.withColumn(stagesCtx, output, op, java_cols)
