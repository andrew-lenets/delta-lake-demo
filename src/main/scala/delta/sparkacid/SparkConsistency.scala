package demo.delta.sparkacid

import demo.delta.SparkConfig.spark

import scala.util.Try

object SparkConsistency extends App {
  val fileName = "delta-demo/consistency"

  import spark.implicits._

  Try {
    spark.range(10, 20)
      .map(i => {
        if (i > 15) throw new RuntimeException("consistency error")
        i
      })
      .toDF("value")
      .write
      .mode("overwrite")
      .format("parquet")
      .save(fileName)
  }

  spark.read
    .format("parquet")
    .load(fileName)
    .show(100)
}
