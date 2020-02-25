import sbt._

object v {
  val spark     = "2.4.3"
  val hadoop    = "2.8.5"
}

object deps {
  // Spark
  val sql              = "org.apache.spark"                %% "spark-sql"                     % v.spark force()

  val delta            = "io.delta"                        %% "delta-core"                    % "0.5.0"

  // logging
  val slf4j            = "org.slf4j"                       %  "slf4j-api"                     % "1.7.25"
  val log4j12          = "org.slf4j"                       %  "slf4j-log4j12"                 % "1.7.16"

  val spark = Seq(sql, delta)
  val logging = Seq(slf4j, log4j12)
}

