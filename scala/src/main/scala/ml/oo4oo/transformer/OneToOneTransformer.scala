package ml.oo4oo.transformer

import ml.combust.mleap.core.types.{NodeShape, StructType, TensorType, TypeSpec}
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, FrameBuilder, Row, RowUtil, Transformer}
import ml.combust.mleap.runtime.function.{FieldSelector, Selector, StructSelector, UserDefinedFunction}
import ml.oo4oo.model.OneToOneModel
import ml.oo4oo.userop.UserOpMode

import scala.util.{Failure, Success, Try}

/*
RowTransformerLearner
RowTransformerModel
OneToOneLearner
OneToOneTransformer


 * */

case class OneToOneTransformer(
  override val uid: String = Transformer.uniqueName("one_to_one_transformer"),
  override val shape: NodeShape,
  override val model: OneToOneModel
) extends Transformer {

  val inputs: Seq[String]      = model.inputSchema.fields.map(_.name)
  val outputs: Seq[String]     = model.outputSchema.fields.map(_.name)
  val selectors: Seq[Selector] = inputs.map(FieldSelector)

  override def transform[FB <: FrameBuilder[FB]](builder: FB): Try[FB] = {
    // println(s"\t\t${uid} mleap transform")
    // println(s"inputs: ${inputs.mkString(",")}, outputs: ${outputs.mkString(",")}")

    val frame = builder.asInstanceOf[DefaultLeapFrame]
    val rows  = frame.collect()

    val typeSpecs = model.inputSchema.fields.map(_.dataType).map(dt => dt: TypeSpec)

    val rowSelectors = selectors
      .zip(typeSpecs)
      .map { case (selector, typeSpec) =>
        RowUtil.createRowSelector(frame.schema, selector, typeSpec)
      }
      .map(_.get)

    val resultRows = rows.map(r => {
      val inputArray  = rowSelectors.map(_(r))
      val outputArray = model.transform(inputArray.toArray)

      if (model.mode() == UserOpMode.REPLACE.toString) {
        ArrayRow(outputArray)
      } else {
        r.withValues(outputArray)
      }
    })

    (if (model.mode() == UserOpMode.REPLACE.toString) {
       StructType(model.outputSchema.fields)
     } else {
       frame.schema.withFields(model.outputSchema.fields)
     }) match {
      case Success(s)  => Success(DefaultLeapFrame(s, resultRows).asInstanceOf[FB])
      case Failure(ex) => Failure(ex)
    }
  }

  /*override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(outputSchema.fields.map(_.name), StructSelector(inputSchema.fields.map(_.name)))(exec)
  }*/

  /*override val exec: UserDefinedFunction = (values: Array[Any]) => {
    Row(model.transform(values): _*)
  }*/
  //    UserDefinedFunction((row: Row) => model(row.toSeq): Double, ScalarType.Double, Seq(SchemaSpec(inputSchema)))
}
