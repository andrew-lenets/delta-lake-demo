package demo.delta.schema

import demo.delta.SparkConfig.spark
import org.apache.spark.sql.DataFrame

object SchemaEnforcement extends App {
  val fileName = "delta-demo/schema"

  import spark.implicits._

  println("Init schema with 2 columns")
  val df1 = Seq(
    (1, "one"),
    (2, "two")
  ).toDF("number", "description")

  write(df1, "overwrite")
  read

  println("Appending data with only first column")
  val df2 = Seq(
    5,
    6
  ).toDF("number")

  write(df2, "append")
  read

  println("Appending data with only second column")
  val df3 = Seq(
    "three",
    "four").toDF("description")

  write(df3, "append")
  read

  println("Extending schema with third column")
  val df4 = Seq(
    (7, "seven", "number 7"),
    (8, "eight", "number 8")
  ).toDF("number", "description", "comment")

  write(df4, "append")
  read

  println("Overwriting data with new schema")
  val df5 = Seq(
    ("value1", "value2"),
    ("value3", "value4")
  ).toDF("new_column1", "new_column2")

  write(df5, "overwrite")
  read

  def write(df: DataFrame, mode: String) = df.write
    .format("parquet")
    .mode(mode)
    .save(fileName)

  def read = spark.read
    .format("parquet")
    .load(fileName)
    .show()
}
