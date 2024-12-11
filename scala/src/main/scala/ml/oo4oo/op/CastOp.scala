package ml.oo4oo.op

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import ml.oo4oo.model.CastModel
import ml.oo4oo.transformer.Cast

class CastOp extends MleapOp[Cast, CastModel] {
  override val Model: OpModel[MleapContext, CastModel] = new OpModel[MleapContext, CastModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[CastModel] = classOf[CastModel]

    // a unique name for our op
    override def opName: String = CastModel.Name

    override def store(model: Model, obj: CastModel)(implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("from_type", Value.basicType(mleapToBundleBasicType(obj.fromType)))
        .withValue("to_type", Value.basicType(mleapToBundleBasicType(obj.toType)))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): CastModel = {
      val fromType = bundleToMleapBasicType(model.value("from_type").getBasicType)
      val toType   = bundleToMleapBasicType(model.value("to_type").getBasicType)

      CastModel(fromType, toType)
    }
  }

  // the core model that is used by the transformer
  override def model(node: Cast): CastModel = node.model
}
