package ml.oo4oo.userop

import ml.bundle.BasicType
import ml.combust.mleap.core.types.{DataType, ScalarShape, ScalarType, StructField, StructType}

case class SelectOp(params: Map[String, Any]) extends UserOpBase {
  override def mode = UserOpMode.REPLACE.toString

  override def outputSchema(
    inputCols: Array[String],
    outputCols: Array[String],
    datasetSchema: Seq[StructField]
  ): StructType = {
    assert(outputCols.size == inputCols.size, s"outputCols should be equal to inputCols")

    inputSchema(inputCols, outputCols, datasetSchema)
  }

  override def transform(row: Array[Any]): Array[Any] = row
}

case class ConcatOp(params: Map[String, Any]) extends UserOpBase {
  override def outputSchema(
    inputCols: Array[String],
    outputCols: Array[String],
    datasetSchema: Seq[StructField]
  ): StructType = {
    assert(outputCols.size == 1, s"outputCols should be equal to 1, but it's '${outputCols.size}'")
    StructType(outputCols(0) -> ScalarType.String.nonNullable).get
  }

  override def transform(row: Array[Any]): Array[Any] = {
    Array(row.map(_.toString()).mkString(""))
  }
}

case class MaxOp(params: Map[String, Any]) extends UserOpBase {
  override def outputSchema(
    inputCols: Array[String],
    outputCols: Array[String],
    datasetSchema: Seq[StructField]
  ): StructType = {
    assert(outputCols.size == 1, s"outputCols should be equal to 1, but it's '${outputCols.size}'")

    val dt: DataType = datasetSchema.filter(_.name == inputCols.head).head.dataType
    StructType(outputCols(0) -> dt).get
  }

  override def transform(row: Array[Any]): Array[Any] = {
    // val re = regexString.r
    // re.replaceAllIn(input, replaceString)

    // val newCol = input.getAs[String](0).toLowerCase()
    // Row.fromSeq(input.toSeq ++ Array[Any](newCol, 1))
    // Row.fromSeq(input.toSeq ++ Array[Any](-1, 1))
    val prefix = params.size.toString
    Array(prefix + ":" + row.map(_.toString()).max)
  }
}

case class DropOp(params: Map[String, Any]) extends UserOpBase {
  override def mode = UserOpMode.REPLACE.toString

  override def inputSchema(
    inputCols: Array[String],
    outputCols: Array[String],
    datasetSchema: Seq[StructField]
  ): StructType = {
    val availableColumns = datasetSchema.map(_.name)
    val inputColsExist   = inputCols.map(availableColumns.contains(_)).reduce(_ && _)
    assert(
      inputColsExist,
      s"inputCols '${inputCols.mkString(",")}' should be exist in the '${availableColumns.mkString(",")}'"
    )

    StructType(datasetSchema.filter(x => !inputCols.contains(x.name))).get
  }

  override def outputSchema(
    inputCols: Array[String],
    outputCols: Array[String],
    datasetSchema: Seq[StructField]
  ): StructType = {
    inputSchema(inputCols, outputCols, datasetSchema)
  }
}

case class LitOp(params: Map[String, Any]) extends UserOpBase {
  override def outputSchema(
    inputCols: Array[String],
    outputCols: Array[String],
    datasetSchema: Seq[StructField]
  ): StructType = {
    assert(outputCols.size == 1, s"outputCols should be equal to 1, but it's '${outputCols.size}'")
    assert(params.contains("value"), s"need to specify the constant value")

    val dt: DataType = ScalarType.String.nonNullable // params.get("value").get.getClass()
    StructType(outputCols(0) -> dt).get
  }

  override def transform(row: Array[Any]): Array[Any] = {
    Array(params.get("value").get)
  }
}
