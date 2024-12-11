package ml.oo4oo.model

import ml.combust.bundle.BundleFile
import ml.oo4oo.dag.JoinOperator
import ml.combust.mleap.core.types.StructField
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import ml.combust.mleap.spark.SimpleSparkSerializer
import ml.oo4oo.utils.SparkCommon
import ml.oo4oo.utils.SparkCommon.spark.implicits._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.mleap.transformer.OneToOneLearner
import org.scalatest.funspec.AnyFunSpec
import resource.managed

import java.io.File

class OneToOneTransformerTest extends AnyFunSpec {
  val _df1     = Seq(
    (1, "Smith, Mr. John", "(541) 471 3918", "20-02-2019"),
    (2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
    (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
    (4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
  ).toDF("id", "name", "phone_number", "date")
  val df1      = SparkCommon.setNullable(_df1, Seq("id", "name", "phoneNumber", "dateJoined"), true)
  val mleapDF1 = SparkCommon.sparkToMleap(df1)

  val _df2     = Seq((1, 100.0, "a"), (2, 80.3, "b"), (3, 90.0, "c"), (5, 10.0, "d")).toDF("id", "score", "level")
  val df2      = SparkCommon.setNullable(_df2, Seq("id", "score", "level"), true)
  val mleapDF2 = SparkCommon.sparkToMleap(df2)

  def defaultStages(): Array[PipelineStage] = {
    Array(
      new OneToOneLearner(
        OneToOneModel(
          opName = "ml.oo4oo.userop.DropOp",
          Map("lr" -> 0.01, "step" -> "abc")
        )
      )
        .setInputCols(Array("name"))
        .setOutputCols(Array.empty[String])
    )
  }

  describe("RowTransformerTest") {
    it("Transformed sample data of employees") {
      println("Transformed sample data of employees")

      val stages        = defaultStages()
      val pipeline      = new Pipeline().setStages(stages)
      val pipelineModel = pipeline.fit(df1)
      val out           = pipelineModel.transform(df1)

      println("origin DF:")
      df1.show()
      println("output:")
      out.show()
      println(pipelineModel.stages.map(x => x).mkString(" -> "))

      assert(true)
    }

    it("Serialising pipeline to spark bundle -> load spark bundle -> transform with spark API") {
      println("Serialising pipeline to spark bundle -> load spark bundle -> transform with spark API")

      val stages        = defaultStages()
      val pipeline      = new Pipeline().setStages(stages)
      val pipelineModel = pipeline.fit(df1)
      new File("/tmp/spark_bundle.zip").delete()
      pipelineModel.write.overwrite().save("/tmp/spark_bundle.zip")

      val pipelineModelLoaded = PipelineModel.load("/tmp/spark_bundle.zip")
      val out                 = pipelineModelLoaded.transform(df1)

      println("origin DF:")
      df1.show()
      println("output:")
      out.show()
      println(pipelineModelLoaded.stages.map(x => x.uid).mkString(" -> "))

      assert(true)
    }

    it("Serialising to mleap bundle -> load mleap bundle -> transform with spark API") {
      println("Serialising to mleap bundle -> load mleap bundle -> transform with spark API")

      val stages        = defaultStages()
      val pipeline      = new Pipeline().setStages(stages)
      val pipelineModel = pipeline.fit(df1)

      val bundlePath = s"jar:file:/tmp/mleap_bundle.zip"
      new File(bundlePath).delete()
      println(s"bundle file: ${bundlePath}")
      new SimpleSparkSerializer().serializeToBundle(
        transformer = pipelineModel,
        path = bundlePath,
        dataset = SparkCommon.spark.emptyDataFrame
      )

      val pipelineLoaded = new SimpleSparkSerializer().deserializeFromBundle(bundlePath)

      val out = df1.transform(pipelineLoaded.transform)
      println("origin DF:")
      df1.show()
      println("output:")
      out.show()

      println(pipelineLoaded.uid)
      assert(true)
    }

    it("Load mleap bundle -> transform with mleap API") {
      println("Load mleap bundle -> transform with mleap API")

      val bundlePath    = s"jar:file:/tmp/mleap_bundle.zip"
      val mleapPipeline = (for (bundleFile <- managed(BundleFile(bundlePath))) yield {
        bundleFile.loadMleapBundle().get.root
      }).tried.get
      println("load mleap bundle file success")

      val out = SparkCommon.mleapToSpark(mleapPipeline.transform(mleapDF1).get)
      println("origin DF:")
      df1.show()
      println("output:")
      out.show()

      assert(true)
    }
  }
}
