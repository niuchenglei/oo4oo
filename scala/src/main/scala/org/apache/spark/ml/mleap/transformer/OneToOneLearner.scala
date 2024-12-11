package org.apache.spark.ml.mleap.transformer

import ml.oo4oo.DEBUG_PRINT
import ml.oo4oo.model.OneToOneModel
import ml.oo4oo.userop.UserOpMode
import ml.oo4oo.utils.Utils
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCols}
import org.apache.spark.ml.util.{
  DefaultParamsReader,
  DefaultParamsWritable,
  DefaultParamsWriter,
  Identifiable,
  MLReadable,
  MLReader,
  MLWritable,
  MLWriter
}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.mleap.TypeConverters

// row transformer
/*
 * row transformer: select, 1->1 transformer, order by
 * group: aggregation function
 * join
 *
 * */

class OneToOneLearner(override val uid: String, val model: OneToOneModel)
    extends Estimator[OneToOneTransformer]
    with Params
    with HasInputCols
    with HasOutputCols
    with DefaultParamsWritable {

  var transformer: OneToOneTransformer = null
  def this(model: OneToOneModel) = {
    this(uid = Identifiable.randomUID(model.opName.split('.').last), model = model)
    this.transformer = new OneToOneTransformer(uid, model).setParent(this)
    this.setInputCols(Array.empty[String])
    this.setOutputCols(Array.empty[String])
  }

  def setInputCols(value: Array[String]): this.type  = {
    this.transformer.setInputCols(value)
    set(inputCols, value)
  }
  def setOutputCols(value: Array[String]): this.type = {
    this.transformer.setOutputCols(value)
    set(outputCols, value)
  }

  override def fit(dataset: Dataset[_]): OneToOneTransformer = {
    if (DEBUG_PRINT) {
      println(s"\t\t${uid} fit")
      println(s"\t\tinputCols: ${$(inputCols).mkString(",")}")
      println(s"\t\tdataset schema: ${dataset.schema.fieldNames.mkString(",")}")
      println("\t\t--\n")
    }

    val mleapSchema = dataset.schema.fields.map(TypeConverters.sparkFieldToMleapField(dataset.toDF, _))
    model.updateSchema($(inputCols), $(outputCols), mleapSchema)

    model.fit(dataset)

    copyValues(this.transformer)
  }

  override def transformSchema(schema: StructType): StructType = {
    if (DEBUG_PRINT) {
      println(s"\t\t${uid} learner transformSchema")
      println(s"\t\tschema: ${schema.fieldNames.mkString(",")}")
      println("\t\t--\n")
    }

    this.transformer.transformSchema(schema)
  }

  override def copy(extra: ParamMap): OneToOneLearner = defaultCopy(extra)

  // override def toString: String = s"${model.getClass.getSimpleName}: uid=$uid"
}

class OneToOneTransformer(override val uid: String, val model: OneToOneModel)
    extends Model[OneToOneTransformer]
    with HasInputCols
    with HasOutputCols
    with MLWritable {

  def this(model: OneToOneModel) = {
    this(uid = Identifiable.randomUID(model.opName.split('.').last), model = model)
    this.setInputCols(Array.empty[String])
    this.setOutputCols(Array.empty[String])
  }

  def setInputCols(value: Array[String]): this.type  = set(inputCols, value)
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    if (DEBUG_PRINT) {
      println(s"\t\t${uid} transform")
      println(s"\t\tdataset schema: ${dataset.schema.fieldNames.mkString(",")}")
      println("\t\t--\n")
    }

    val mleapSchema = dataset.schema.fields.map(TypeConverters.sparkFieldToMleapField(dataset.toDF, _))
    model.updateSchema($(inputCols), $(outputCols), mleapSchema)

    val newSchema       = this.transformSchema(dataset.schema)
    val newEncoder      = RowEncoder(newSchema)
    val inputFieldIndex = model.inputSchema.fields.map(x => dataset.schema.fieldIndex(x.name)).toArray

    dataset.map { row =>
      val rowWithSchema = row.asInstanceOf[GenericRowWithSchema] // This cast might not always be possible!
      val rowSelector   = inputFieldIndex.map(rowWithSchema(_))

      val result = model.transform(rowSelector)
      val output =
        if (model.mode() == UserOpMode.REPLACE.toString) {
          result
        } else {
          rowWithSchema.toSeq.toArray ++ result
        }

      new GenericRowWithSchema(output, newSchema)
        .asInstanceOf[Row] // Encoder is invariant so we have to cast again.
    }(newEncoder)
  }

  override def copy(extra: ParamMap): OneToOneTransformer =
    copyValues(new OneToOneTransformer(uid, model), extra).setParent(parent)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    if (DEBUG_PRINT) {
      println(s"\t\t${uid} transformer transformSchema")
      println(
        s"\t\t${schema.fieldNames.mkString(",")}, inputCols: ${$(inputCols).mkString(",")}, outputCols: ${$(outputCols).mkString(",")}"
      )
      println("\t\t--\n")
    }

    if (model.mode() == UserOpMode.APPEND.toString) {
      $(outputCols).foreach(outputCol =>
        require(!schema.fieldNames.contains(outputCol), s"Output column '${outputCol}' already exists.")
      )
    }
    $(inputCols).foreach(inputCol =>
      require(
        schema.fieldNames.contains(inputCol),
        s"InputCol column '${inputCol}' should exists in [${schema.fieldNames.mkString(",")}]."
      )
    )

    val mleapSchema = schema.fields.map(MleapTypeConverters.sparkFieldToMleapField(_))
    model.updateSchema($(inputCols), $(outputCols), mleapSchema)

    val outputSchema = model.outputSchema.fields.map(TypeConverters.mleapFieldToSparkField).toArray
    if (model.mode() == UserOpMode.REPLACE.toString) {
      StructType(outputSchema)
    } else {
      StructType(schema.fields ++ outputSchema)
    }
  }

  override def write: MLWriter = new OneToOneTransformer.RowTransformerWriter(this)

  // override def toString: String = s"${model.getClass.getSimpleName}: uid=$uid"
}

object OneToOneTransformer extends MLReadable[OneToOneTransformer] {
  private case class Data(opName: String, params: String, memory: String)

  private class RowTransformerWriter(instance: OneToOneTransformer) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      val data     = Data(
        opName = instance.model.opName,
        params = Utils.toJson(instance.model.params),
        memory = instance.model.serialise()
      )
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class RowTransformerReader extends MLReader[OneToOneTransformer] {
    private val className = classOf[OneToOneTransformer].getName

    override def load(path: String): OneToOneTransformer = {
      val metadata    = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath    = new Path(path, "data").toString
      val data        = sparkSession.read
        .parquet(dataPath)
        .head()
      val opName      = data.getAs[String](0)
      val params      = Utils.fromJson(data.getAs[String](1))
      val memory      = data.getAs[String](2)
      val model       = OneToOneModel(opName, params)
      model.deserialise(memory)
      val transformer = new OneToOneTransformer(metadata.uid, model)
      metadata.getAndSetParams(transformer)

      transformer
    }
  }

  override def read: MLReader[OneToOneTransformer] = new RowTransformerReader

  override def load(path: String): OneToOneTransformer = super.load(path)
}

/* *
How's the `Learner.fit, Learner.transformSchema, Transformer.transform, Transformer.transformSchema` works?
For example, we have following two Ops, `ConcatOp` and `SelectOp`, and the `df1` have columns `id,name,phone_number,date`

```
val deviceFeatures = PipelineDAGHelper
  .placeHolder("df1")
  .withColumn("features", "ConcatOp", Array("name", "phone_number"))
  .select("id", "name", "phone_number", "date", "features")
  .toNode()
```

The process is:

1. fit
  ml.oo4oo.userop.ConcatOp_c51af079b9b6 learner transformSchema (schema=id,name,phone_number,date)
  ml.oo4oo.userop.SelectOp_6517ba7ce156 learner transformSchema (schema=id,name,phone_number,date)
  ml.oo4oo.userop.ConcatOp_c51af079b9b6 fit (dataset=id,name,phone_number,date)
  ml.oo4oo.userop.ConcatOp_c51af079b9b6 transform (dataset=id,name,phone_number,date)
  ml.oo4oo.userop.SelectOp_6517ba7ce156 fit (dataset=id,name,phone_number,date,features)

2. transform
  ml.oo4oo.userop.ConcatOp_c51af079b9b6 transformer transformSchema (schema=id,name,phone_number,date)
  ml.oo4oo.userop.SelectOp_6517ba7ce156 transformer transformSchema (schema=id,name,phone_number,date,features)
  ml.oo4oo.userop.ConcatOp_c51af079b9b6 transform (dataset=id,name,phone_number,date)
  ml.oo4oo.userop.SelectOp_6517ba7ce156 transform (dataset=id,name,phone_number,date,features)

 * */
