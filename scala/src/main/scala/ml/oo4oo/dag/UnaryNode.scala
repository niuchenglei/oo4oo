package ml.oo4oo.dag

import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer => MleapTransformer}
import ml.combust.mleap.spark.SimpleSparkSerializer
import ml.oo4oo.DEBUG_PRINT
import ml.oo4oo.utils.Utils
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol, HasOutputCols}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import resource.managed

import java.io.File
import scala.reflect.ClassTag
import scala.reflect.io.Directory

case class UnaryNode(input: String, _uid: String = "") extends Node {
  override val inputs = Seq(input)
  override val uid    = if (_uid.isEmpty) Utils.uuid("una", input) else _uid

  var pipeline: Pipeline                 = null
  var sparkPipelineModel: PipelineModel  = null
  var mleapTransformer: MleapTransformer = null
  // var _sparkSession: SparkSession        = null

  override def fit(in_0: DataFrame, in_1: DataFrame): Unit = {
    // _sparkSession = in_0.sparkSession
    val model = pipeline.fit(in_0)
    sparkPipelineModel = model
  }

  override def transform[T: ClassTag](in_0: T, in_1: T): T = {
    in_0 match {
      case x: DataFrame        =>
        assert(sparkPipelineModel != null, "the spark pipeline is empty, please fit or load the pipeline first")
        sparkPipelineModel.transform(x).asInstanceOf[T]
      case x: DefaultLeapFrame =>
        assert(mleapTransformer != null, "the mleap pipeline is empty, please fit or load the pipeline first")
        mleapTransformer.transform(x).get.asInstanceOf[T]
      case _                   => throw new RuntimeException(s"not support data type")
    }
  }

  override def save(basePath: String): Unit = {
    val dir = new Directory(new File(basePath))
    dir.createDirectory()

    // save it to mleap and spark bundle
    val sparkBundlePath = s"${basePath}/spark_bundle.zip"
    val mleapBundlePath = s"${basePath}/mleap_bundle.zip"
    new File(sparkBundlePath).delete()
    new File(mleapBundlePath).delete()

    sparkPipelineModel.write.overwrite().save(sparkBundlePath)

    // since there is some error while call `save` from python, let's move persistMleap to load
    // persistMleap(mleapBundlePath)
  }

  def persistMleap(mleapBundlePath: String) = {
    if (DEBUG_PRINT) {
      println("persistMleap")
    }

    val spark: SparkSession = SparkSession
      .builder()
      .appName("sample-app")
      .getOrCreate()
    new SimpleSparkSerializer().serializeToBundle(
      transformer = sparkPipelineModel,
      path = s"jar:file:${mleapBundlePath}",
      dataset = spark.emptyDataFrame
    )
  }

  override def load(basePath: String): UnaryNode = {
    val sparkBundlePath = s"${basePath}/spark_bundle.zip"
    val mleapBundlePath = s"${basePath}/mleap_bundle.zip"

    sparkPipelineModel = PipelineModel.load(sparkBundlePath)
    pipeline = new Pipeline().setStages(sparkPipelineModel.stages)

    // since there is some error while call `save` from python, let's move persistMleap to load
    persistMleap(mleapBundlePath)

    if (DEBUG_PRINT) {
      println("load mleapTransformer")
    }
    mleapTransformer = (for (bundleFile <- managed(BundleFile(s"jar:file:${mleapBundlePath}"))) yield {
      bundleFile.loadMleapBundle().get.root
    }).tried.get
    this
  }

  override def describe(): String = {
    pipeline.getStages.zipWithIndex
      .map { case (x, idx) =>
        val in  =
          if (x.isInstanceOf[HasInputCols]) x.asInstanceOf[HasInputCols].getInputCols
          else if (x.isInstanceOf[HasInputCol]) Array(x.asInstanceOf[HasInputCol].getInputCol)
          else Array.empty[String]
        val out =
          if (x.isInstanceOf[HasOutputCols]) x.asInstanceOf[HasOutputCols].getOutputCols
          else if (x.isInstanceOf[HasOutputCol]) Array(x.asInstanceOf[HasOutputCol].getOutputCol)
          else Array.empty[String]

        val offset = pipeline.getStages.size - idx - 1
        "\t" * offset + s"${x.uid}(input=[${in.mkString(",")}], output=[${out.mkString(",")}])"
      }
      .reverse
      .mkString("\n")
  }
}
