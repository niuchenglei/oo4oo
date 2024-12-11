package ml.oo4oo.spark.op

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.oo4oo.model.OneToOneModel
import ml.oo4oo.utils.Utils
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.transformer.OneToOneTransformer

// this class aim to write to mleap bundle from spark pipeline model
class OneToOneTransformerOp extends OpNode[SparkBundleContext, OneToOneTransformer, OneToOneModel] {
  override val Model: OpModel[SparkBundleContext, OneToOneModel] =
    new OpModel[SparkBundleContext, OneToOneModel] {
      override val klazz: Class[OneToOneModel] = classOf[OneToOneModel]

      override def opName: String = "one_to_one_transformer"

      override def store(model: Model, obj: OneToOneModel)(implicit
        context: BundleContext[SparkBundleContext]
      ): Model = {
        // println(s"${this.getClass.getName} store, ${obj.opName}")
        val memory = obj.serialise()
        model
          .withValue("opName", Value.string(obj.opName))
          .withValue("params", Value.string(Utils.toJson(obj.params)))
          .withValue("memory", Value.string(memory))
      }

      override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): OneToOneModel = {
        // usually will not call this function since we don't need to load a mleap bundle to a spark context, we directly load spark pipeline
        val opName = model.value("opName").getString
        val params = Utils.fromJson(model.value("params").getString)
        val memory = model.value("memory").getString
        val ins    = new OneToOneModel(opName, params)
        ins.deserialise(memory)

        ins
      }
    }

  override val klazz: Class[OneToOneTransformer] = classOf[OneToOneTransformer]

  override def name(node: OneToOneTransformer): String = node.uid

  override def model(node: OneToOneTransformer): OneToOneModel = node.model

  override def load(node: Node, model: OneToOneModel)(implicit
    context: BundleContext[SparkBundleContext]
  ): OneToOneTransformer = {
    new OneToOneTransformer(uid = node.name, model = model)
      .setInputCols(node.shape.inputs.map(_.name).toArray)
      .setOutputCols(node.shape.outputs.map(_.name).toArray)
  }

  override def shape(node: OneToOneTransformer)(implicit context: BundleContext[SparkBundleContext]): NodeShape = {
    var ns = NodeShape()
    ns = node.getInputCols.foldLeft(ns) { case (_ns, inputCol) =>
      _ns.withInput(inputCol, inputCol)
    }
    ns = node.getOutputCols.foldLeft(ns) { case (_ns, outputCol) =>
      _ns.withOutput(outputCol, outputCol)
    }
    ns
  }
}
