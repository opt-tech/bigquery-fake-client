import sbt._

object Dependencies {
  val bigquery = "com.google.cloud" % "google-cloud-bigquery" % "1.76.0"
  val storage = "com.google.cloud" % "google-cloud-storage" % "1.76.0"
  val parser = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
  val jsqlparser = "com.github.jsqlparser" % "jsqlparser" % "0.9.6"
  val postgres = "org.postgresql" % "postgresql" % "9.4.1211"
  val h2 = "com.h2database" % "h2" % "1.4.193"
  val jawn = "org.spire-math" %% "jawn-ast" % "0.10.4"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.0"
}
