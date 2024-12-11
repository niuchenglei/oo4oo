package ml.oo4oo.example

import ml.combust.mleap.core.types.BasicType
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.oo4oo.model.CastModel
import ml.oo4oo.utils.SparkCommon
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.mleap.transformer._

object MleapTransformerExample {

  def buildDataFrames(): (DataFrame, DefaultLeapFrame) = {
    import SparkCommon.spark.implicits._

    val _df1     = Seq(
      (1, "Smith, Mr. John", "(541) 471 3918", 1.0f),
      (2, "Davis, Ms. Nicole", "(603)281-0308", 2.1f),
      (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", 3.1f),
      (4, "Armstrong, Dr. Sam", "9704443106", 4.1f)
    ).toDF("id", "name", "phone_number", "score")
    val df1      = SparkCommon.setNullable(_df1, Seq("id", "name", "phone_number", "score"), true)
    val mleapDF1 = SparkCommon.sparkToMleap(df1)
    (df1, mleapDF1)
  }

  def buildTransformers(): Seq[PipelineStage] = {
    val s1 =
      new Cast(new CastModel(BasicType.Int, BasicType.String)).setInputCol("id").setOutputCol("id_str")
    val s2 =
      new Cast(new CastModel(BasicType.Double, BasicType.String)).setInputCol("score").setOutputCol("score_str")

    Seq(s1, s2)
  }

  def main(args: Array[String]): Unit = {
    val (df1, mleapDF1) = buildDataFrames()
    val stages          = buildTransformers()

    val pipeline      = new Pipeline().setStages(stages.toArray)
    val pipelineModel = pipeline.fit(df1)

    println("fit done")

    val result = pipelineModel.transform(df1)
    result.show()
  }
}
