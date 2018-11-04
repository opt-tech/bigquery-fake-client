import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      crossScalaVersions := Seq("2.10.7", "2.11.12", "2.12.4"),
      licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/bigquery-fake-driver/master/LICENSE"),
      organization := "jp.ne.opt",
      scalaVersion := "2.12.5",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "bigquery-fake-driver",
    libraryDependencies += scalaTest % Test,
    publishMavenStyle := true
  )
