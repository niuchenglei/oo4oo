package ml.oo4oo.example

import ml.combust.mleap.spark.SimpleSparkSerializer
import ml.oo4oo.model.OneToOneModel
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.mleap.transformer.OneToOneLearner
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

import java.io.File

case class CaseClass(uid: String) {
  var memory: Map[String, Any] = Map.empty[String, Any]

  def addNode(param: Map[String, Any]): Unit = {
    memory ++= param
  }

  def getMemory(): Map[String, Any] = memory

  def runSpark(): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("sample-app")
      .getOrCreate()

    import spark.implicits._

    Seq((1, "charlie"), (2, "niu")).toDF("id", "name")
  }

  def runDF(df: DataFrame): DataFrame = {
    df.withColumn("runDF", F.lit("ok"))
  }

  def runString(): String = uid
}

class ExampleClass(uid: String) {
  var memory: Map[String, Any] = Map.empty[String, Any]

  def addNode(param: Map[String, Any]): Unit = {
    memory ++= param
  }

  def getMemory(): Map[String, Any] = memory

  def runSpark(): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("sample-app")
      .getOrCreate()

    import spark.implicits._

    Seq((1, "charlie"), (2, "niu")).toDF("id", "name")
  }

  def runDF(df: DataFrame, value: String): DataFrame = {
    df.withColumn("runDF", F.lit(value))
  }

  def runDFs(dfs: Map[String, DataFrame], value: String): Map[String, DataFrame] = {
    dfs.map { case (k, v) =>
      k -> v.withColumn("runDF", F.lit(value))
    }
  }

  def runArray(input: Seq[String]): Seq[String] = {
    input ++ Seq("from_scala")
  }

  def runString(): String = uid

  def runPipeline(df: DataFrame): Unit = {
    val cols                 = Array("id", "name")
    val stage: PipelineStage = new OneToOneLearner(
      OneToOneModel(opName = "ml.oo4oo.userop.SelectOp")
    )
      .setInputCols(cols)
      .setOutputCols(Array.empty[String])
    val pipeline             = new Pipeline().setStages(Array(stage))
    val sparkPipelineModel   = pipeline.fit(df)

    val sparkBundlePath = s"/tmp/test_example/spark_bundle.zip"
    val mleapBundlePath = s"/tmp/test_example/mleap_bundle.zip"
    new File(sparkBundlePath).delete()
    new File(mleapBundlePath).delete()
    println("save spark bundle")
    sparkPipelineModel.write.overwrite().save(sparkBundlePath)

    /*val spark: SparkSession = SparkSession
      .builder()
      .appName("sample-app")
      .getOrCreate()*/

    println("save mleap bundle")
    new SimpleSparkSerializer().serializeToBundle(
      transformer = sparkPipelineModel,
      path = s"jar:file:${mleapBundlePath}",
      dataset = df
    )
  }
}

object Example {
  def runInt(): Int = 0

  def runString(): String = "ok"

  def run(param: Map[String, Any]): String = "ok"

  def getNode(): CaseClass = CaseClass("uid")

  def runSpark(): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("sample-app")
      .getOrCreate()

    import spark.implicits._

    Seq((1, "charlie"), (2, "niu")).toDF("id", "name")
  }

  def runDF(df: DataFrame): DataFrame = {
    df.withColumn("runDF", F.lit("ok"))
  }
}
