import sbt._

object Dependencies {
  lazy val parser = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
  lazy val jsqlparser = "com.github.jsqlparser" % "jsqlparser" % "0.9.6"
  lazy val postgres = "org.postgresql" % "postgresql" % "9.4.1211"
  lazy val h2 = "com.h2database" % "h2" % "1.4.193"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}
