name := "KafkaSpark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1",
  "com.twitter" % "chill_2.11" % "0.8.0",
  "com.twitter" % "chill-bijection_2.11" % "0.8.0",
  "tv.cntt" % "chill-scala_2.11" % "1.2"
)
    