package ml.oo4oo.example

import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.oo4oo.dag.PipelineDAG
import ml.oo4oo.utils.SparkCommon
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Seq

object LoadPythonBundleExample {
  def buildDataFrames(): (DataFrame, DataFrame, DefaultLeapFrame, DefaultLeapFrame) = {
    import SparkCommon.spark.implicits._

    val _df1     = Seq(
      (1, "Smith, Mr. John", "(541) 471 3918", "20-02-2019"),
      (2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
      (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
      (4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
    ).toDF("id", "name", "phone_number", "date")
    val df1      = SparkCommon.setNullable(_df1, Seq("id", "name", "phone_number", "date"), true)
    val mleapDF1 = SparkCommon.sparkToMleap(df1)

    val _df2     = Seq((1, 100.0, "a"), (2, 80.3, "b"), (3, 90.0, "c"), (5, 10.0, "d")).toDF("id", "score", "level")
    val df2      = SparkCommon.setNullable(_df2, Seq("id", "score", "level"), true)
    val mleapDF2 = SparkCommon.sparkToMleap(df2)

    (df1, df2, mleapDF1, mleapDF2)
  }

  def dagLoad(path: String): Unit = {
    println(s"dag load")
    val (df1, df2, mleapDF1, mleapDF2) = buildDataFrames()

    val inputs      = Map("df1" -> df1, "df2" -> df2)
    val mleapInputs = Map("df1" -> mleapDF1, "df2" -> mleapDF2)
    val dag         = PipelineDAG.load(path)

    val outputSpark = dag.transform[DataFrame](inputs)
    println("spark output:")
    outputSpark.show()

    val outputMleap = dag.transform[DefaultLeapFrame](mleapInputs)
    println("mleap output:")
    outputMleap.show()
  }

  def main(args: Array[String]): Unit = {
    dagLoad("/tmp/dag_by_python")
  }
}
