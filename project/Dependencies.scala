import sbt._

object Dependencies {
  val bigquery = "com.google.cloud" % "google-cloud-bigquery" % "1.76.0"
  val storage = "com.google.cloud" % "google-cloud-storage" % "1.76.0"
  val parser = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
  val jsqlparser = "com.github.jsqlparser" % "jsqlparser" % "0.9.6"
  val postgres = "org.postgresql" % "postgresql" % "9.4.1211"
  val h2 = "com.h2database" % "h2" % "1.4.193"
  val jawn = "org.typelevel" %% "jawn-ast" % "0.14.2"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.8"
  val junit = "junit" % "junit" % "4.12"
  val junitInterface = "com.novocode" % "junit-interface" % "0.11"
  val mockito = "org.mockito" % "mockito-core" % "2.28.2"
}
