package ml.oo4oo.dag

import ml.oo4oo.utils.Serialize
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

trait BinaryOperator extends Serialize {
  def fit(in_0: DataFrame, in_1: DataFrame): Unit = {}
  def transform[T: ClassTag](in_0: T, in_1: T): T = in_0
}
