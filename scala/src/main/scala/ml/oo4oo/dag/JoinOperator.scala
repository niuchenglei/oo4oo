package ml.oo4oo.dag

import ml.combust.mleap.core.types.TypeSpec
import ml.combust.mleap.runtime.frame.Row.RowSelector
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, Row, RowUtil}
import ml.combust.mleap.runtime.function.FieldSelector
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.typeOf

case class JoinOperator(left: String, right: String, usingColumns: Seq[String], joinType: String) extends BinaryOperator {
  override def transform[T: ClassTag](left: T, right: T): T = {
    (left, right) match {
      case (x: DataFrame, y: DataFrame)               => x.join(y, usingColumns, joinType).asInstanceOf[T]
      case (x: DefaultLeapFrame, y: DefaultLeapFrame) =>
        if (joinType == "right" | joinType == "right_outer")
          joinMleapFrame(x, y, usingColumns, joinType).asInstanceOf[T]
        else joinMleapFrame(x, y, usingColumns, joinType).asInstanceOf[T]
      case _                                          => throw new RuntimeException(s"not support data type")
    }
  }

  private def joinMleapFrame(
    left: DefaultLeapFrame,
    right: DefaultLeapFrame,
    usingColumns: Seq[String],
    joinType: String
  ): DefaultLeapFrame = {
    val leftKeySelectors   = getRowSelectors(left, usingColumns)
    val rightKeySelectors  = getRowSelectors(right, usingColumns)
    // val leftColsSelectors  = getRowSelectors(left, left.schema.fields.map(_.name))
    val rightColsSelectors = getRowSelectors(right, right.schema.fields.map(_.name).diff(usingColumns))

    val keyIndexMap = right.dataset.zipWithIndex.map { case (r, idx) =>
      val md5Key = rightKeySelectors.map(_(r)).map(_.toString).mkString("")
      md5Key -> idx
    }.toMap

    // Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'.
    var rightTableMatchedIndex = Seq.empty[String]
    val resultRows             = left.dataset
      .map(r => {
        val md5Key          = leftKeySelectors.map(_(r)).map(_.toString).mkString("")
        val rightTableIndex = keyIndexMap.getOrElse(md5Key, -1)

        joinType match {
          case "left" | "left_outer"  =>
            if (rightTableIndex >= 0) {
              val rightRow  = right.dataset(rightTableIndex)
              val rightCols = rightColsSelectors.map(_(rightRow))
              r.withValues(rightCols)
            } else {
              r.withValues(Array.fill(rightColsSelectors.size) {
                null
              })
            }
          case "join"                 =>
            if (rightTableIndex >= 0) {
              val rightRow  = right.dataset(rightTableIndex)
              val rightCols = rightColsSelectors.map(_(rightRow))
              r.withValues(rightCols)
            } else {
              null
            }
          case "outer" | "full_outer" =>
            if (rightTableIndex >= 0) {
              rightTableMatchedIndex = rightTableMatchedIndex :+ md5Key
              val rightRow  = right.dataset(rightTableIndex)
              val rightCols = rightColsSelectors.map(_(rightRow))
              r.withValues(rightCols)
            } else {
              r.withValues(Array.fill(rightColsSelectors.size) {
                null
              })
            }
        }
      })
      .filter(_ != null)

    val fullJoinAdditionRows = if (Seq("full_outer", "outer").contains(joinType)) {
      right.dataset
        .map(r => {
          val keyArray = rightKeySelectors.map(_(r))
          val md5Key   = keyArray.map(_.toString).mkString("")
          if (rightTableMatchedIndex.contains(md5Key)) {
            null
          } else {
            val leftTableNullSize     = left.schema.fields.size - usingColumns.size
            val nullArray: Array[Any] = Array.fill(leftTableNullSize) {
              null
            }
            val rightArray            = rightColsSelectors.map(_(r))
            ArrayRow(keyArray ++ nullArray ++ rightArray)
          }
        })
        .filter(_ != null)
    } else {
      Seq.empty[Row]
    }

    val newColsSchema = right.schema.fields.filter(x => !usingColumns.contains(x.name))
    val newSchema     = left.schema.withFields(newColsSchema).get
    DefaultLeapFrame(newSchema, resultRows ++ fullJoinAdditionRows)
  }

  private def getRowSelectors(df: DefaultLeapFrame, cols: Seq[String]): Seq[RowSelector] = {
    cols
      .map(x => {
        val dt: TypeSpec = df.schema.fields.filter(_.name == x).head.dataType
        RowUtil.createRowSelector(df.schema, FieldSelector(x), dt)
      })
      .map(_.get)
  }

}
