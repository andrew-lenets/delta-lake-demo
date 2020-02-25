package demo.delta.updatedelete

import demo.delta.SparkConfig.spark
import io.delta.tables.DeltaTable

object DeltaUpdate extends App {
  val location = "delta-demo/events"

  val deltaTable = DeltaTable.forPath(spark, location)

  deltaTable.updateExpr(
    "ts >= '1582382525' AND ts <= '1582382526' AND event = 'update'",
    Map(("event", "'update_updated'"))
  )

  spark.read
    .format("delta")
    .load(location)
    .show(100)
}

