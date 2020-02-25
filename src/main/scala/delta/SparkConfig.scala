package demo.delta

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkConfig {
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.parquet").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder
    .appName("Delta Lake App")
    .config("spark.master", "local[1]")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    //    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()
}
