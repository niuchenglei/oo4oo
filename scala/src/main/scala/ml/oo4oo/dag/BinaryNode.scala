package ml.oo4oo.dag

import ml.oo4oo.utils.{Serialize, Utils}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag
import scala.reflect.io.Directory

case class BinaryNode(override val inputs: Seq[String], _uid: String = "") extends Node {
  var operator: BinaryOperator = null
  override val uid       = if (_uid.isEmpty) Utils.uuid("bin", inputs) else _uid

  override def fit(in_0: DataFrame, in_1: DataFrame): Unit = operator.fit(in_0, in_1)

  override def transform[T: ClassTag](in_0: T, in_1: T): T = {
    assert(inputs.size == 2, "only two inputs are supported")
    operator.transform(in_0, in_1)
  }

  override def save(basePath: String): Unit = {
    val dir = new Directory(new File(basePath))
    dir.createDirectory()

    val filePath = s"${basePath}/metadata.txt"
    val mm       = Utils.toJson(
      Map("operator" -> operator.toString(), "operator_type" -> operator.getClass.toString.split(" ").last)
    )
    Files.write(Paths.get(filePath), mm.getBytes(StandardCharsets.UTF_8))
  }

  override def load(basePath: String): BinaryNode = {
    val filePath = s"${basePath}/metadata.txt"
    val objStr   = Files.readString(Paths.get(filePath), StandardCharsets.UTF_8)
    val mm       = Utils.fromJson(objStr)

    operator = Serialize.fromString[BinaryOperator](mm("operator").asInstanceOf[String])
    this
  }

  override def describe(): String = {
    val msg = if (operator.isInstanceOf[JoinOperator]) {
      val op = operator.asInstanceOf[JoinOperator]
      s"key=[${op.usingColumns.mkString(",")}], type=${op.joinType}"
    }

    s"${operator.getClass.getSimpleName}(left=${inputs(0)}, right=${inputs(1)}, ${msg})"
  }
}
