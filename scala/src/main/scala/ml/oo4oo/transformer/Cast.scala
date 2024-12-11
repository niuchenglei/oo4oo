package ml.oo4oo.transformer

import ml.combust.mleap.core.types.{BasicType, NodeShape}
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.oo4oo.model.CastModel

class Cast(
  override val uid: String = Transformer.uniqueName(CastModel.Name),
  override val shape: NodeShape,
  override val model: CastModel
) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (model.fromType, model.toType) match {
    case (BasicType.Int, BasicType.String)     => (value: Int) => model.intToString(value)
    case (BasicType.Double, BasicType.String)  => (value: Double) => model.doubleToString(value)
    case (BasicType.String, BasicType.Int)     => (value: String) => model.stringToInt(value)
    case (BasicType.String, BasicType.Double)  => (value: String) => model.stringToDouble(value)
    case (BasicType.Int, BasicType.Double)     => (value: Int) => model.intToDouble(value)
    case (BasicType.Int, BasicType.Boolean)    => (value: Int) => model.intToBoolean(value)
    case (BasicType.Boolean, BasicType.Double) => (value: Boolean) => model.boolToDouble(value)
    case (BasicType.Long, BasicType.Double)    => (value: Long) => model.longToDouble(value)
    case unk                                   => throw new IllegalArgumentException(s"unsupported cast type $unk")
  }
}
