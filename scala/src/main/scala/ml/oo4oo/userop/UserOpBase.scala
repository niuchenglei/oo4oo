package ml.oo4oo.userop

import ml.combust.mleap.core.types.{DataType, ScalarType, StructField, StructType}
import ml.oo4oo.utils.Utils
import org.apache.spark.sql.Dataset

trait UserOpBase {
  val memory = scala.collection.mutable.Map.empty[String, Any]

  def mode: UserOpMode.UserOpMode = UserOpMode.APPEND.toString()

  def fit(dataset: Dataset[_]): Map[String, Any] = Map.empty[String, String]

  def transform(input: Array[Any]): Array[Any] = input

  def inputSchema(inputCols: Array[String], outputCols: Array[String], datasetSchema: Seq[StructField]): StructType = {
    val availableColumns = datasetSchema.map(_.name)
    val inputColsExist   = if (inputCols.size > 0) inputCols.map(availableColumns.contains(_)).reduce(_ && _) else true
    assert(
      inputColsExist,
      s"inputCols '${inputCols.mkString(",")}' should be exist in the '${availableColumns.mkString(",")}'"
    )

    StructType(inputCols.map(x => datasetSchema.filter(y => y.name == x).head)).get
  }

  def outputSchema(inputCols: Array[String], outputCols: Array[String], datasetSchema: Seq[StructField]): StructType

  def updateSchema(inputCols: Array[String], outputCols: Array[String], datasetSchema: Seq[StructField]): Unit = {
    memory ++= Map(
      "__input_schema"  -> inputSchema(inputCols, outputCols, datasetSchema),
      "__output_schema" -> outputSchema(inputCols, outputCols, datasetSchema)
    )
  }

  def serialise(): String = {
    val inputSchemaEncode  = Utils.structTypeEncode(memory("__input_schema").asInstanceOf[StructType])
    val outputSchemaEncode = Utils.structTypeEncode(memory("__output_schema").asInstanceOf[StructType])
    val mmToSave           = memory.filter(!_._1.startsWith("__")).toMap ++ Map(
      "__input_schema"  -> inputSchemaEncode,
      "__output_schema" -> outputSchemaEncode
    )

    Utils.toJson(mmToSave)
  }

  def deserialise(str: String): UserOpBase = {
    val mmToRestore  = Utils.fromJson(str)
    val inputSchema  = Utils.structTypeDecode(mmToRestore("__input_schema").asInstanceOf[String])
    val outputSchema = Utils.structTypeDecode(mmToRestore("__output_schema").asInstanceOf[String])

    memory ++= mmToRestore.filter(!_._1.startsWith("__")) ++ Map(
      "__input_schema"  -> inputSchema,
      "__output_schema" -> outputSchema
    )
    this
  }

  def getInputSchema() = memory.getOrElse("__input_schema", StructType()).asInstanceOf[StructType]

  def getOutputSchema() = memory.getOrElse("__output_schema", StructType()).asInstanceOf[StructType]
}

object UserOpMode extends Enumeration {
  type UserOpMode = String
  val APPEND  = Value("APPEND")
  val REPLACE = Value("REPLACE")

  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
}
