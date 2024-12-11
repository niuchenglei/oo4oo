package org.apache.spark.ml.mleap.transformer

import ml.bundle
import ml.combust.mleap.core.types.BasicType
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import ml.oo4oo.model.CastModel
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.types.{StructField, StructType}

class Cast(override val uid: String, val model: CastModel)
    extends Transformer
    with HasInputCol
    with HasOutputCol
    with MLWritable {

  val castUdf = (model.fromType, model.toType) match {
    case (BasicType.Int, BasicType.String)     => udf((value: Int) => model.intToString(value))
    case (BasicType.Double, BasicType.String)  => udf((value: Double) => model.doubleToString(value))
    case (BasicType.String, BasicType.Int)     => udf((value: String) => model.stringToInt(value))
    case (BasicType.String, BasicType.Double)  => udf((value: String) => model.stringToDouble(value))
    case (BasicType.Int, BasicType.Double)     => udf((value: Int) => model.intToDouble(value))
    case (BasicType.Int, BasicType.Boolean)    => udf((value: Int) => model.intToBoolean(value))
    case (BasicType.Boolean, BasicType.Double) => udf((value: Boolean) => model.boolToDouble(value))
    case (BasicType.Long, BasicType.Double)    => udf((value: Long) => model.longToDouble(value))
    case unk                                   => throw new IllegalArgumentException(s"unsupported cast type $unk")
  }

  def this(model: CastModel) =
    this(uid = Identifiable.randomUID(CastModel.Name), model = model)

  def setInputCol(value: String): this.type  = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // println(s"\t\t${uid} transformer transformSchema")
    // println(s"\t\t${dataset.schema.fieldNames.mkString(",")}")

    dataset.withColumn($(outputCol), castUdf(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer = copyValues(new Cast(uid, model), extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // println(s"\t\t${uid} transformer transformSchema")
    // println(s"\t\t${schema.fieldNames.mkString(",")}")

    require(isSet(inputCol), s"Input column ${$(inputCol)} are required")

    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(outputCol)), s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), TypeConverters.mleapBasicTypeToSparkType(model.toType)))
  }

  override def write: MLWriter = new Cast.CastWriter(this)
}

object Cast extends MLReadable[Cast] {

  override def read: MLReader[Cast] = new CastReader

  override def load(path: String): Cast = super.load(path)

  private class CastWriter(instance: Cast) extends MLWriter {

    private case class Data(fromTypeName: String, toTypeName: String)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: fromTypeName, toTypeName
      val model    = instance.model
      val data     = Data(mleapToBundleBasicType(model.fromType).name, mleapToBundleBasicType(model.toType).name)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CastReader extends MLReader[Cast] {

    /** Checked against metadata when loading model */
    private val className = classOf[Cast].getName

    override def load(path: String): Cast = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath     = new Path(path, "data").toString
      val data         = sparkSession.read.parquet(dataPath).select("fromTypeName", "toTypeName").head()
      val fromTypeName = data.getAs[String](0)
      val toTypeName   = data.getAs[String](1)

      val model = CastModel(bundle.BasicType.fromName(fromTypeName).get, bundle.BasicType.fromName(toTypeName).get)
      val cast  = new Cast(metadata.uid, model)

      metadata.getAndSetParams(cast)
      cast
    }
  }
}
