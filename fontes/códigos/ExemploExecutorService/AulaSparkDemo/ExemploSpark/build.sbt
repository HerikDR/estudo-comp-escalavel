name := "AulaSparkDemo"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.slf4j" % "slf4j-simple" % "2.0.9"
)