package ml.oo4oo.utils

import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, types}

object SparkCommon {
  val spark: SparkSession = if (false) {
    // initialize cluster spark
    // SparkSession.builder().config(sparkConf).getOrCreate()
    SparkSession
      .builder()
      .appName("sample-app")
      .getOrCreate()
  } else {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-session")
      .config("spark.sql.shuffle.partitions", "1")
      // .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      // .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()
  }

  import spark.implicits._

  def setNullable(df: DataFrame, cols: Seq[String], nullable: Boolean): DataFrame = {
    // get schema
    val schema    = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if cols.contains(c) => types.StructField(c, t, nullable = nullable, m)
      case y: StructField                              => y
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def createSampleData(): DataFrame = {
    val data = Seq(
      (1, "Smith, Mr. John", "(541) 471 3918", "20-02-2019"),
      (2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
      (3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
      (4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
    ).toDF("id", "name", "phone_number", "date")

    setNullable(data, Seq("id", "name", "phone_number", "date"), true)
  }

  def sparkToMleap(df: DataFrame): DefaultLeapFrame = {
    val schema = TypeConverters.sparkSchemaToMleapSchema(df)
    DefaultLeapFrame(
      schema,
      df.collect
        .map { sparkRow: org.apache.spark.sql.Row =>
          ml.combust.mleap.runtime.frame.Row(org.apache.spark.sql.Row.unapplySeq(sparkRow).get: _*)
        }
    )
  }

  def mleapToSpark(df: DefaultLeapFrame): DataFrame = {
    val schema = TypeConverters.mleapSchemaToSparkSchema(df.schema)
    val rows   = df.dataset
      .map { mleapRow: ml.combust.mleap.runtime.frame.Row =>
        org.apache.spark.sql.Row(mleapRow.toSeq: _*)
      }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }
}
