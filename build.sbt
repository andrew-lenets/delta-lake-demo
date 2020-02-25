import deps._

name := "delta-lake-demo"

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1"

libraryDependencies ++=
  logging ++
    spark
