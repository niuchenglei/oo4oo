package ml.oo4oo.model

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.oo4oo.userop.{UserOpBase, UserOpMode}
import org.apache.spark.sql.Dataset

case class OneToOneModel(opName: String, params: Map[String, Any] = Map()) extends Model {
  lazy val opInstance: UserOpBase = {
    val clazz       = Class.forName(opName)
    val constructor = clazz.getConstructor(classOf[Map[String, Any]])
    constructor.newInstance(params).asInstanceOf[UserOpBase]
  }

  def mode(): UserOpMode.UserOpMode = opInstance.mode

  def fit(dataset: Dataset[_]): Unit = opInstance.memory ++= opInstance.fit(dataset)

  def transform(row: Array[Any]): Array[Any] = opInstance.transform(row)

  def inputSchema: StructType = opInstance.getInputSchema()

  def outputSchema: StructType = opInstance.getOutputSchema()

  def updateSchema(inputCols: Array[String], outputCols: Array[String], schema: Seq[StructField]): Unit =
    opInstance.updateSchema(inputCols, outputCols, schema)

  def serialise(): String = opInstance.serialise()

  def deserialise(str: String) = opInstance.deserialise(str)
}
