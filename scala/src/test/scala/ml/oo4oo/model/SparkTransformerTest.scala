package ml.oo4oo.model

import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SimpleSparkSerializer
import ml.oo4oo.utils.SparkCommon
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.ml.mleap.transformer.OneToOneLearner
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util.{Identifiable, MLWritable}
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funspec.AnyFunSpec
import resource.managed
import ml.oo4oo.utils.SparkCommon
import ml.oo4oo.utils.SparkCommon.spark.implicits._

import java.io.File

class Transformer1(override val uid: String = Identifiable.randomUID("row_transformer"))
    extends Transformer
    with HasInputCols
    with HasOutputCols {

  def setInputCols(value: Array[String]): this.type  = set(inputCols, value)
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    println(s"\t\ttransform1")
    dataset.withColumn($(outputCols)(0), F.lit(1))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    println(s"\t\ttransformSchema1")
    println(s"schema: ${schema.fieldNames.mkString(",")}")

    $(outputCols).foreach(outputCol =>
      require(!schema.fieldNames.contains(outputCol), s"Output column '${outputCol}' already exists.")
    )
    $(inputCols).foreach(inputCol =>
      require(
        schema.fieldNames.contains(inputCol),
        s"InputCol column '${inputCol}' should exists in [${schema.fieldNames.mkString(",")}]."
      )
    )

    val outputSchema = Array(StructField($(outputCols)(0), IntegerType))
    StructType(schema.fields ++ outputSchema)
  }

  override def copy(extra: ParamMap): Transformer1 = copyValues(new Transformer1(uid), extra)
}

class Transformer2(override val uid: String = Identifiable.randomUID("row_transformer"))
    extends Transformer
    with HasInputCols
    with HasOutputCols {

  def setInputCols(value: Array[String]): this.type  = set(inputCols, value)
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    println(s"\t\ttransform2")
    dataset.withColumn($(outputCols)(0), F.lit("abc"))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    println(s"\t\ttransformSchema2")
    println(s"schema: ${schema.fieldNames.mkString(",")}")

    $(outputCols).foreach(outputCol =>
      require(!schema.fieldNames.contains(outputCol), s"Output column '${outputCol}' already exists.")
    )
    $(inputCols).foreach(inputCol =>
      require(
        schema.fieldNames.contains(inputCol),
        s"InputCol column '${inputCol}' should exists in [${schema.fieldNames.mkString(",")}]."
      )
    )

    val outputSchema = Array(StructField($(outputCols)(0), StringType))
    StructType(schema.fields ++ outputSchema)
  }

  override def copy(extra: ParamMap): Transformer2 = copyValues(new Transformer2(uid), extra)
}

class SparkTransformerTest extends AnyFunSpec {
  val _df1 = Seq(
    (1, "Smith, Mr. John", "(541) 471 3918", "20-02-2019"),
    (2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
    (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
    (4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
  ).toDF("id", "name", "phone_number", "date")
  val df1  = SparkCommon.setNullable(_df1, Seq("id", "name", "phoneNumber", "dateJoined"), true)

  def defaultStages(): Array[PipelineStage] = {
    Array(
      new Transformer1()
        .setInputCols(Array("name"))
        .setOutputCols(Array("o1")),
      new Transformer2()
        .setInputCols(Array("name"))
        .setOutputCols(Array("o2"))
    )
  }

  describe("SparkTransformerTest") {
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
  }
}
