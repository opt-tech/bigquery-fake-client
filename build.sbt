import Dependencies._
import Helpers._
import com.typesafe.tools.mima.core.{ProblemFilters, ReversedMissingMethodProblem}

fork in Test := true

val scala211 = "2.11.12"

scalaVersion := scala211

crossScalaVersions := Seq(scala211, "2.12.13", "2.13.5")

name := "bigquery-fake-client"

licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/bigquery-fake-client/master/LICENSE")

organization := "jp.ne.opt"

version := "0.1.0"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (compileScope(bigquery, storage, jawn, jsqlparser) ++
  compileScope(parser) ++
  testScope(postgres, h2, scalatest, junit, junitInterface exclude("junit", "junit-dep"), mockito) ++
  providedScope(postgres, h2))

publishMavenStyle := true
publishTo := Some(Opts.resolver.sonatypeStaging)

mimaPreviousArtifacts ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, v)) if v >= 13 =>
      Set.empty
    case Some((epoch, minor)) =>
      Set(organization.value % s"${name.value}_${epoch}.${minor}" % "0.1.0")
  }
}

mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[ReversedMissingMethodProblem]("jp.ne.opt.bigqueryfake.rewriter.RewriteMode.matches")
)
