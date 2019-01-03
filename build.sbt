import Dependencies._
import Helpers._

val scala210 = "2.10.7"

scalaVersion := scala210

crossScalaVersions := Seq(scala210, "2.11.12", "2.12.8")

name := "bigquery-fake-client"

licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/bigquery-fake-client/master/LICENSE")

organization := "jp.ne.opt"

version := "0.1.0-SNAPSHOT"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (compileScope(bigquery, storage, jawn, jsqlparser) ++
  (if (scalaVersion.value.startsWith("2.10")) Nil else compileScope(parser)) ++
  testScope(postgres, h2, scalatest) ++
  providedScope(postgres, h2))

publishMavenStyle := true
