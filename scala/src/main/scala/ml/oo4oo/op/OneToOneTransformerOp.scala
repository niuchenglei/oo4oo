package ml.oo4oo.op

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.oo4oo.model.OneToOneModel
import ml.oo4oo.transformer.OneToOneTransformer
import ml.oo4oo.utils.Utils

// this class aim to load the mleap bundle to support transform on mleap dataframe
class OneToOneTransformerOp extends MleapOp[OneToOneTransformer, OneToOneModel] {
  override val Model: OpModel[MleapContext, OneToOneModel] = new OpModel[MleapContext, OneToOneModel] {
    override val klazz: Class[OneToOneModel] = classOf[OneToOneModel]

    override def opName: String = "one_to_one_transformer"

    override def store(model: Model, obj: OneToOneModel)(implicit context: BundleContext[MleapContext]): Model = {
      // usually will not call this function since we don't need to save a bundle from mleap context
      // println(s"${this.getClass.getName} store, ${obj.opName}")

      val memory = obj.serialise()
      model
        .withValue("opName", Value.string(obj.opName))
        .withValue("params", Value.string(Utils.toJson(obj.params)))
        .withValue("memory", Value.string(memory))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): OneToOneModel = {
      val opName = model.value("opName").getString
      val params = Utils.fromJson(model.value("params").getString)
      val memory = model.value("memory").getString
      val ins    = new OneToOneModel(opName, params)
      ins.deserialise(memory)
      // println(s"${this.getClass.getName} load, ${ins.opName}")

      ins
    }
  }

  override def model(node: OneToOneTransformer): OneToOneModel = node.model

}
