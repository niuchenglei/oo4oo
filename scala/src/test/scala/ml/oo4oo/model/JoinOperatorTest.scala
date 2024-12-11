package ml.oo4oo.model

import ml.oo4oo.dag.JoinOperator
import ml.oo4oo.utils.SparkCommon
import ml.oo4oo.utils.SparkCommon.spark.implicits._
import org.scalatest.funspec.AnyFunSpec

class JoinOperatorTest extends AnyFunSpec {

  val _df1     = Seq(
    (1, "Smith, Mr. John", "(541) 471 3918", "20-02-2019"),
    (2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
    (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
    (4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
  ).toDF("id", "name", "phone_number", "date")
  val df1      = SparkCommon.setNullable(_df1, Seq("id", "name", "phoneNumber", "dateJoined"), true)
  val mleapDF1 = SparkCommon.sparkToMleap(df1)

  val _df2     = Seq((1, 100.0, "a"), (2, 80.3, "b"), (3, 90.0, "c"), (5, 10.0, "d")).toDF("id", "score", "level")
  val df2      = SparkCommon.setNullable(_df2, Seq("id", "score", "level"), true)
  val mleapDF2 = SparkCommon.sparkToMleap(df2)

  describe("JoinOperator Test") {
    it("1") {
      val op  = JoinOperator("left", "right", Seq("id"), "inner")
      val out = op.transform(df1, df2)
      out.show()

      assert(true)
    }
  }
}
