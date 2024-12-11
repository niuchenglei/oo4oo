package org.apache.spark.ml.mleap.transformer

import ml.combust.mleap.core.types
import org.apache.spark.ml.linalg.{MatrixUDT, VectorUDT}
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.types._

import scala.language.implicitConversions
import scala.util.Try

object MleapTypeConverters {
  private def getVectorSize(field: StructField): Int = {
    val sizeInMeta = Try(field.metadata.getMetadata("ml_attr").getLong("num_attrs").toInt)
    if (sizeInMeta.isSuccess) {
      sizeInMeta.get
    } else {
      123
    }
  }

  def sparkFieldToMleapField(field: StructField): types.StructField = {
    val dt = field.dataType match {
      case BooleanType                                               => types.ScalarType.Boolean
      case ByteType                                                  => types.ScalarType.Byte
      case ShortType                                                 => types.ScalarType.Short
      case IntegerType                                               => types.ScalarType.Int
      case LongType                                                  => types.ScalarType.Long
      case FloatType                                                 => types.ScalarType.Float
      case DoubleType                                                => types.ScalarType.Double
      case _: DecimalType                                            => types.ScalarType.Double
      case StringType                                                => types.ScalarType.String.setNullable(field.nullable)
      case ArrayType(ByteType, _)                                    => types.ListType.Byte
      case ArrayType(BooleanType, _)                                 => types.ListType.Boolean
      case ArrayType(ShortType, _)                                   => types.ListType.Short
      case ArrayType(IntegerType, _)                                 => types.ListType.Int
      case ArrayType(LongType, _)                                    => types.ListType.Long
      case ArrayType(FloatType, _)                                   => types.ListType.Float
      case ArrayType(DoubleType, _)                                  => types.ListType.Double
      case ArrayType(StringType, _)                                  => types.ListType.String
      case ArrayType(ArrayType(ByteType, _), _)                      => types.ListType.ByteString
      case MapType(keyType, valueType, _)                            =>
        types.MapType(
          TypeConverters.sparkTypeToMleapBasicType(keyType),
          TypeConverters.sparkTypeToMleapBasicType(valueType)
        )
      case _: VectorUDT                                              =>
        val size = getVectorSize(field)
        types.TensorType.Double(size)
      case _: MatrixUDT                                              =>
        types.TensorType.Double(12, 13)
      case ArrayType(elementType, _) if elementType == new VectorUDT =>
        types.TensorType.Double(12, 13)
      case _                                                         => throw new UnsupportedOperationException(s"Cannot convert spark field $field to mleap")
    }

    types.StructField(field.name, dt.setNullable(field.nullable))
  }
}
