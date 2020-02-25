package demo.delta.sparkacid

import demo.delta.SparkConfig.spark

import scala.util.Try

object SparkAtomicity extends App {
  val fileName = "delta-demo/atomicity"

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val goodRange = spark.range(10)
    .toDF("value")
    .withColumn("partition", lit("goodRange"))

  val badRange = spark.range(10, 20)
    .map(i => {
      if (i > 15) throw new RuntimeException("atomicity error");
      i
    })
    .toDF("value")
    .withColumn("partition", lit("badRange"))

  Try {
    goodRange.union(badRange)
      .write
      .partitionBy("partition")
      .mode("append")
      .format("parquet")
      .save(fileName)
  }

  spark.read
    .format("parquet")
    .load(fileName)
    .show(100)
}
