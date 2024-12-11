import os

from enum import Enum


from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCol
from pyspark.ml.param.shared import HasOutputCol
from pyspark.ml.param.shared import Param
from pyspark.ml.param.shared import Params
from pyspark.ml.param.shared import TypeConverters
from pyspark.ml.util import JavaMLReadable
from pyspark.sql import DataFrame
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.util import _jvm
from pyspark.ml.wrapper import JavaWrapper, JavaParams
from pyspark.ml.common import inherit_doc, _java2py, _py2java

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


from oo4oo.pyspark import classpath_jars
from oo4oo.pyspark.py2scala import jvm_scala_object

from datetime import datetime, date
from pyspark.sql import Row


#from oo4oo.pyspark.py2scala import jvm_scala_object
#from oo4oo.pyspark.py2scala import ScalaNone
#from oo4oo.pyspark.py2scala import Some


def with_spark_context():
    os.environ['SPARK_CLASSPATH'] = ":".join(classpath_jars())
    conf = SparkConf().set("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH'])
    if SparkContext._active_spark_context is None:
        SparkContext(conf=conf)
    yield SparkContext._active_spark_context
    SparkContext.stop(SparkContext._active_spark_context)


class ExampleClass(JavaWrapper, JavaMLWritable):
    def __init__(self):
        """
        Computes the mathematical binary `operation` over
        the input columns A and B.
        """
        super(ExampleClass, self).__init__()

        # if operation=None, it means that pyspark is reloading the model
        # from disk and calling this method without args. In such case we don't
        # need to set _java_obj here because pyspark will set it after creation
        #
        # if operation is not None, we can proceed to instantiate the scala classes

        #self.exampleObject = jvm_scala_object(_jvm().ml.oo4oo.dag, f"Example$")

        #self.caseClass = _jvm().ml.oo4oo.dag.CaseClass("uid_caseclass")
        self._java_obj = self._new_java_obj("ml.oo4oo.example.ExampleClass", "example_class_name")

        #self._setDefault()
        #self.setParams(inputA=inputA, inputB=inputB, outputCol=outputCol)

    def runString(self):
        v = self._java_obj.runString()
        print(f"runString return:{v}:{type(v)}")

    def addNode(self, params):
        java_param = _jvm().PythonUtils.toScalaMap(params) #self._transfer_param_map_to_java(params)
        self._java_obj.addNode(java_param)

    def getMemory(self):
        mem = self._java_obj.getMemory()
        py_dict = dict(_jvm().scala.collection.JavaConversions.mapAsJavaMap(mem))
        print(f"getMemory: {str(py_dict)}:{type(py_dict)}")

    def runSpark(self, df: DataFrame):
        java_df = self._java_obj.runDF(df._jdf, "str_from_python")
        output = DataFrame(java_df, df.sparkSession)
        output.show()

    def runSparkMap(self, dfs: dict[str, DataFrame]):
        input = _jvm().PythonUtils.toScalaMap({k:v._jdf for k,v in dfs.items()})
        java_dfs = self._java_obj.runDFs(input, "str_from_python")
        py_dict = dict(_jvm().scala.collection.JavaConversions.mapAsJavaMap(java_dfs))

        py_dfs = {k:DataFrame(v, spark) for k,v in py_dict.items()}
        for k, v in py_dfs.items():
            print(k)
            v.show()

    def runArray(self, arr: list[str]):
        java_input = _jvm().PythonUtils.toSeq(arr)
        out = list(_jvm().scala.collection.JavaConversions.seqAsJavaList(self._java_obj.runArray(java_input)))
        print(', '.join(out))

    def runPipeline(self, df: DataFrame):
        return self._java_obj.runPipeline(df._jdf)

class CaseClass(JavaWrapper, JavaMLWritable):
    def __init__(self, uid):
        """
        Computes the mathematical binary `operation` over
        the input columns A and B.
        """
        super(CaseClass, self).__init__()

        # if operation=None, it means that pyspark is reloading the model
        # from disk and calling this method without args. In such case we don't
        # need to set _java_obj here because pyspark will set it after creation
        #
        # if operation is not None, we can proceed to instantiate the scala classes

        #self.exampleObject = jvm_scala_object(_jvm().ml.oo4oo.dag, f"Example$")

        #self.caseClass = _jvm().ml.oo4oo.dag.CaseClass("uid_caseclass")
        self._java_obj = self._new_java_obj("ml.oo4oo.example.CaseClass", uid)

        #self._setDefault()
        #self.setParams(inputA=inputA, inputB=inputB, outputCol=outputCol)

    def runString(self):
        v = self._java_obj.runString()
        print(f"runString return:{v}:{type(v)}")

    def addNode(self, params):
        java_param = _jvm().PythonUtils.toScalaMap(params) #self._transfer_param_map_to_java(params)
        self._java_obj.addNode(java_param)

    def getMemory(self):
        mem = self._java_obj.getMemory()
        py_dict = dict(_jvm().scala.collection.JavaConversions.mapAsJavaMap(mem))
        print(f"getMemory: {str(py_dict)}:{type(py_dict)}")

    def runSpark(self, df):
        java_df = self._java_obj.runDF(df._jdf)
        output = DataFrame(java_df, df.sparkSession)
        output.show()

class ObjectClass(JavaWrapper, JavaMLWritable):
    def __init__(self, uid):
        """
        Computes the mathematical binary `operation` over
        the input columns A and B.
        """
        super(ObjectClass, self).__init__()

        # if operation=None, it means that pyspark is reloading the model
        # from disk and calling this method without args. In such case we don't
        # need to set _java_obj here because pyspark will set it after creation
        #
        # if operation is not None, we can proceed to instantiate the scala classes

        #self.exampleObject = jvm_scala_object(_jvm().ml.oo4oo.dag, f"Example$")

        #self.caseClass = _jvm().ml.oo4oo.dag.CaseClass("uid_caseclass")
        self._java_obj = jvm_scala_object(_jvm().ml.oo4oo.example, f"Example$")
        #self._new_java_obj("ml.oo4oo.dag.Example")

        #self._setDefault()
        #self.setParams(inputA=inputA, inputB=inputB, outputCol=outputCol)

    def runString(self):
        v = self._java_obj.runString()
        print(f"runString return:{v}:{type(v)}")

    def run(self, params):
        java_param = _jvm().PythonUtils.toScalaMap(params) #self._transfer_param_map_to_java(params)
        self._java_obj.run(java_param)

    def getNode(self):
        node = self._java_obj.getNode()
        java_param = _jvm().PythonUtils.toScalaMap({"key": "v1", "k2": 2.03})
        node.addNode(java_param)
        mem = node.getMemory()
        py_dict = dict(_jvm().scala.collection.JavaConversions.mapAsJavaMap(mem))
        print(f"getNode: {str(py_dict)}:{type(py_dict)}")

    def runSpark(self, df):
        java_df = self._java_obj.runDF(df._jdf)
        output = DataFrame(java_df, df.sparkSession)
        output.show()

def testClass(df1: DataFrame, df2: DataFrame):
    ins = ExampleClass()
    #ins.runString()
    #ins.runSpark(df1)
    #ins.runSparkMap({"df1":df1, "df2":df2})
    ins.runArray(["a", "b", "c"])
    ins.runPipeline(df1)
    #ins.addNode({"key": "v1", "k2": 2.03})
    #ins.getMemory()


def testCaseClass(df: DataFrame):
    ins = CaseClass("case_class_name")
    ins.runString()
    ins.runSpark(df)
    ins.addNode({"key": "v1", "k2": 2.03})
    ins.getMemory()

def testObject(df: DataFrame):
    ins = ObjectClass("object_name")
    ins.runString()
    ins.runSpark(df)
    ins.getNode()
    ins.run({"key": "v1", "k2": 2.03})

if __name__=="__main__":
    #export PYTHONPATH=.

    #with_spark_context()

    os.environ['SPARK_CLASSPATH'] = ":".join(classpath_jars())
    conf = SparkConf().set("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']) #.set("spark.jars", os.environ['SPARK_CLASSPATH'])
    sc = SparkContext("local", "JavaWrapperExample", conf=conf)

    spark = SparkSession.builder.getOrCreate()
    print(spark.conf.get("spark.driver.extraClassPath"))

    df1 = spark.createDataFrame([
        (1, "Smith, Mr. John", "(541) 471 3918", "20-02-2019"),
        (2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
        (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
        (4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
    ], schema='id long, name string, phone_number string, date String')
    df1.show()

    df2 = spark.createDataFrame([
        (1, 2., 'charlie'),
        (2, 3., 'jackson'),
        (3, 4., 'kk'),
    ], schema='id long, score double, name string')
    df2.show()

    testClass(df1, df2)
    #testCaseClass(df)
    #testObject(df)
