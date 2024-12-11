package ml.oo4oo.dag

import ml.oo4oo.utils.{Serialize, Utils}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.reflect.ClassTag

trait Node extends Serialize {
  val uid: String
  val inputs: Seq[String]                         = Seq.empty[String]
  def fit(in_0: DataFrame, in_1: DataFrame): Unit = {}
  def transform[T: ClassTag](in_0: T, in_1: T): T = in_0
  def save(basePath: String): Unit                = {}
  def load(basePath: String): Node                = this
  def describe(): String                          = ""
}

case class StagesCtx(stages: Array[PipelineStage], node: Node, ctx: Map[String, Node])

case class NodeMetadata(uid: String, inputs: Seq[String], className: String)

case class NodesMetadata(nodes: Seq[NodeMetadata]) extends Serialize
