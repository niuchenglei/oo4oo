package ml.oo4oo.model

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ScalarType, StructType}

object CastModel {
  final val Name = "cast"
}

case class CastModel(fromType: BasicType, toType: BasicType) extends Model {

  def intToString(input: Int): String       = input.toString
  def doubleToString(input: Double): String = input.toString
  def stringToInt(input: String): Int       = input.toInt
  def stringToDouble(input: String): Double = input.toDouble
  def intToDouble(input: Int): Double       = input.toDouble
  def intToBoolean(input: Int): Boolean     = if (input != 0) true else false
  def boolToDouble(input: Boolean): Double  = if (input) 1.0 else 0.0
  def longToDouble(input: Long): Double     = input.toDouble

  override def inputSchema: StructType = StructType("input" -> ScalarType(fromType)).get

  override def outputSchema: StructType = StructType("output" -> ScalarType(toType)).get
}
