import sys, os
from oo4oo.pyspark.pipeline_dag import PipelineDAGHelper

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from oo4oo.pyspark import classpath_jars

from datetime import datetime, date

def define_dag():
    helper = PipelineDAGHelper()

    n1 = helper.placeHolder("df1")
    n2 = helper.withColumn(n1, "features", "ConcatOp", ["name", "phone_number"])
    n3 = helper.select(n2, ["id", "name", "phone_number", "date", "features"])
    n4 = helper.toNode(n3)

    a1 = helper.placeHolder("df2")
    a2 = helper.select(a1, ["id", "score", "level"])
    a3 = helper.toNode(a2)

    b1 = helper.join(n4, a3, ["id"], "full_outer")
    b2 = helper.select(b1, ["id", "name", "phone_number", "date", "score", "features"])
    b3 = helper.toNode(b2)

    return helper.apply(b3)


if __name__=="__main__":
    #export PYTHONPATH=.

    os.environ['SPARK_CLASSPATH'] = ",".join(classpath_jars())
    conf = SparkConf().set("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']).set("spark.jars", os.environ['SPARK_CLASSPATH'])
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
        (1, 100.0, "a"),
        (2, 80.3, "b"),
        (3, 90.0, "c"),
        (5, 10.0, "d")
    ], schema='id long, score double, level string')
    df2.show()

    dag = define_dag()

    input = {"df1": df1, "df2": df2}
    dag.fit(input)
    output = dag.transform(input)
    output.show()

    dag.save("/tmp/dag_by_python")