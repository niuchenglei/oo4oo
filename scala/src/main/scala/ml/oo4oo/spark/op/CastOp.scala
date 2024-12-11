package ml.oo4oo.spark.op

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, NodeShape, Value}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import ml.oo4oo.model.CastModel
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.transformer.Cast

class CastOp extends OpNode[SparkBundleContext, Cast, CastModel] {
  override val Model: OpModel[SparkBundleContext, CastModel] =
    new OpModel[SparkBundleContext, CastModel] {
      override val klazz: Class[CastModel] = classOf[CastModel]

      override def opName: String = CastModel.Name

      override def store(model: Model, obj: CastModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
        model
          .withValue("from_type", Value.basicType(mleapToBundleBasicType(obj.fromType)))
          .withValue("to_type", Value.basicType(mleapToBundleBasicType(obj.toType)))
      }

      override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): CastModel = {
        val fromType = bundleToMleapBasicType(model.value("from_type").getBasicType)
        val toType   = bundleToMleapBasicType(model.value("to_type").getBasicType)
        CastModel(fromType = fromType, toType = toType)
      }
    }

  override val klazz: Class[Cast] = classOf[Cast]

  override def name(node: Cast): String = node.uid

  override def model(node: Cast): CastModel = node.model

  override def load(node: Node, model: CastModel)(implicit context: BundleContext[SparkBundleContext]): Cast = {
    new Cast(uid = node.name, model = model)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: Cast)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)

}
