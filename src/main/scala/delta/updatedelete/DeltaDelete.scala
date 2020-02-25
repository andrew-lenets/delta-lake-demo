package demo.delta.updatedelete

import io.delta.tables.DeltaTable
import demo.delta.SparkConfig.spark

object DeltaDelete extends App {
  val location = "delta-demo/events"

  val deltaTable = DeltaTable.forPath(spark, location)

  deltaTable.delete("ts = '1582382526' AND event = 'delete'")

  spark.read
    .format("delta")
    .load(location)
    .show(100)
}
