package demo.delta.timetravel

import demo.delta.SparkConfig.spark

object DeltaTimeTravel extends App {
  val location = "delta-demo/events"

  println(s"version 0:")
  spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(location)
    .show()

  println(s"version 1:")
  spark.read
    .format("delta")
    .load(location + "@v1")
    .show()

  println(s"version 2:")
  spark.read
    .format("delta")
    .option("timestampAsOf", "2020-02-22 14:44:46")
    .load(location)
    .show()
}



