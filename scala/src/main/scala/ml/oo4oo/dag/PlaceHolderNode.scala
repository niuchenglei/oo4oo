package ml.oo4oo.dag

import ml.oo4oo.utils.Utils

case class PlaceHolderNode(input: String, _uid: String = "") extends Node {
  override val inputs = Seq(input)
  override val uid    = if (_uid.isEmpty) Utils.uuid("phd", input) else _uid

  override def describe(): String = {
    s"${inputs.mkString(",")}"
  }
}
