package demo.delta.timetravel

import io.delta.tables.DeltaTable
import demo.delta.SparkConfig.spark

object DeltaAudit extends App {
  val location = "delta-demo/events"

  val deltaTable = DeltaTable.forPath(spark, location)
  deltaTable.history()
    .show(100, false)
}
