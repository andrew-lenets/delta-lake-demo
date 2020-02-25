package demo.delta.sparkacid

import demo.delta.SparkConfig.spark
import org.apache.spark.sql.functions._

import scala.util.Try

object SparkReader extends App {
  val fileName = "delta-demo/isolation"

  while (true) {
    val read = Try(spark.read
      .format("parquet")
      .load(fileName))

    read.map(df => {
      df.show(100)
      println(df.count())
    }).getOrElse(println("nothing to read yet..."))

    Thread.sleep(500)
  }
}

object SparkWriter extends App {
  val fileName = "delta-demo/isolation"

  import spark.implicits._

  delayedRange(0, 5, "write1")
    .union(delayedRange(5, 10, "write2")
      .union(delayedRange(10, 15, "write3")))
    .write
    .partitionBy("partition")
    .mode("append")
    .format("parquet")
    .save(fileName)

  def delayedRange(start: Int, end: Int, partitionValue: String) = spark
    .range(start, end)
    .map(i => {
      Thread.sleep(500);
      i
    })
    .withColumn("partition", lit(partitionValue))
}


