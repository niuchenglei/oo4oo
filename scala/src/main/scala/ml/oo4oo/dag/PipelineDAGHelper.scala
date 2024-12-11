package ml.oo4oo.dag

import ml.combust.mleap.core.types.BasicType
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.oo4oo.model.{CastModel, OneToOneModel}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.mleap.transformer.{Cast, OneToOneLearner, OneToOneTransformer}
import ml.oo4oo.dag._
import org.apache.spark.sql.DataFrame

import java.util.UUID
import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.universe.TypeTag

object PipelineDAGHelper {
  def apply(stagesCtx: StagesCtx): PipelineDAG = PipelineDAG(stagesCtx)

  def load(basePath: String): PipelineDAG = PipelineDAG.load(basePath)

  def placeHolder(input: String): StagesCtx = {
    val node = PlaceHolderNode(input)
    StagesCtx(Array.empty[PipelineStage], node, Map(node.uid -> node))
  }

  def select(stages: StagesCtx, cols: Seq[String]): StagesCtx = {
    val stage: PipelineStage = new OneToOneLearner(OneToOneModel(opName = "ml.oo4oo.userop.SelectOp"))
      .setInputCols(cols.toArray)
      .setOutputCols(cols.toArray)
    StagesCtx(stages.stages :+ stage, stages.node, stages.ctx)
  }

  def drop(stages: StagesCtx, cols: Seq[String]): StagesCtx = {
    val stage: PipelineStage =
      new OneToOneLearner(OneToOneModel(opName = "ml.oo4oo.userop.DropOp"))
        .setInputCols(cols.toArray)
    StagesCtx(stages.stages :+ stage, stages.node, stages.ctx)
  }

  def withMLeapOp(stagesCtx: StagesCtx, stages: Array[PipelineStage]): StagesCtx = {
    StagesCtx(stagesCtx.stages ++ stages, stagesCtx.node, stagesCtx.ctx)
  }

  def toNode(stages: StagesCtx): StagesCtx = {
    val newNode = new UnaryNode(input = stages.node.uid)
    newNode.pipeline = new Pipeline().setStages(stages.stages)
    StagesCtx(Array.empty[PipelineStage], newNode, stages.ctx ++ Map(newNode.uid -> newNode))
  }

  def join(left: StagesCtx, right: StagesCtx, usingColumns: Seq[String], joinType: String): StagesCtx = {
    assert(left.stages.size == 0, "left node stages size is not 0, need `.toNode` to convert left query to node.")
    assert(right.stages.size == 0, "right node stages size is not 0, need `.toNode` to convert right query to node.")

    val leftName  = left.node.uid
    val rightName = right.node.uid
    val joinNode  = new BinaryNode(inputs = Array(leftName, rightName))
    joinNode.operator = new JoinOperator(leftName, rightName, usingColumns, joinType)

    StagesCtx(Array.empty[PipelineStage], joinNode, left.ctx ++ right.ctx ++ Map(joinNode.uid -> joinNode))
  }

  def withColumn(
    stagesCtx: StagesCtx,
    output: String,
    op: String,
    inputs: Seq[String],
    params: Map[String, Any]
  ): StagesCtx = {
    val stage: PipelineStage = new OneToOneLearner(OneToOneModel(opName = s"ml.oo4oo.userop.${op}", params))
      .setInputCols(inputs.toArray)
      .setOutputCols(Array(output))
    StagesCtx(stagesCtx.stages :+ stage, stagesCtx.node, stagesCtx.ctx)
  }

  implicit class PipelineDAGExtension(stagesCtx: StagesCtx) {
    def select(col: String, cols: String*): StagesCtx = {
      PipelineDAGHelper.select(stagesCtx, Array(col) ++ cols)
    }

    def drop(col: String, cols: String*): StagesCtx = {
      PipelineDAGHelper.drop(stagesCtx, Array(col) ++ cols)
    }

    def withMLeapOp(stages: Array[PipelineStage]): StagesCtx = {
      PipelineDAGHelper.withMLeapOp(stagesCtx, stages)
    }

    def join(right: StagesCtx, usingColumns: Seq[String], joinType: String): StagesCtx = {
      PipelineDAGHelper.join(stagesCtx, right, usingColumns, joinType)
    }

    def toNode(): StagesCtx = {
      PipelineDAGHelper.toNode(stagesCtx)
    }

    def withColumn(
      output: String,
      op: String,
      inputs: Seq[String],
      params: Map[String, Any] = Map.empty[String, Any]
    ): StagesCtx = {
      PipelineDAGHelper.withColumn(stagesCtx, output, op, inputs, params)
    }
  }

}
