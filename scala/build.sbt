

organization := "com.sap.mugglmenzel"

name := "spark-experiments"

version := "0.1-SNAPSHOT"

lazy val root = project in file(".")

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0"
)

mainClass in (Compile, run) := Some("SparkExperiments")