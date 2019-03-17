import Dependencies._
import Helpers._

val scala211 = "2.11.12"

scalaVersion := scala211

crossScalaVersions := Seq(scala211, "2.12.8")

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
  compileScope(parser) ++
  testScope(postgres, h2, scalatest) ++
  providedScope(postgres, h2))

publishMavenStyle := true
