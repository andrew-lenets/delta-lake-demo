package demo.delta.updatedelete

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import demo.delta.SparkConfig.spark
import org.apache.spark.sql.types.StructType

import scala.language.postfixOps
import scala.util.Random

object EventGenerator extends App {
  val location = "delta-demo/events"
  val eventSchema = new StructType()
    .add("event", "string")
    .add("ts", "long")
    .add("formatted_ts", "string")

  import spark.implicits._

  (1 to 30 toDF).map(_ => {
    Thread.sleep(100)
    generateEvent
  }).toDF("event", "ts", "formatted_ts")
    .write
    .format("delta")
    .partitionBy("ts")
    .save(location)

  spark.read
    .format("delta")
    .load(location)
    .show(100)

  def generateEvent = {
    val eventTypes = List("create", "delete", "update", "open")
    val eventValue = eventTypes(Random.nextInt(eventTypes.length))
    val datetime = LocalDateTime.now()
    val timestamp = datetime.toEpochSecond(ZoneOffset.UTC)
    val formattedTs = datetime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))

    (eventValue, timestamp, formattedTs)
  }
}
