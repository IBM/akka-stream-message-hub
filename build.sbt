import sbtrelease.ReleaseStateTransformations._
import sbtrelease._
import xerial.sbt.Sonatype._

name := "akka-stream-message-hub"
organization := "com.ibm"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("https://github.com/IBM/akka-stream-message-hub"))

sonatypeProjectHosting := Some(GitHubHosting("IBM", "akka-stream-message-hub", "rafal.bigaj@pl.ibm.com"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/IBM/akka-stream-message-hub"),
    "scm:git@github.com:IBM/akka-stream-message-hub.git"
  )
)

developers := List(
  Developer(
    id    = "rafalbigaj",
    name  = "Rafał Bigaj",
    email = "rafal.bigaj@pl.ibm.com",
    url   = url("https://github.com/rafalbigaj")
  )
)

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.13",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",
  "com.typesafe.akka" %% "akka-http" % "10.0.11",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)

test in assembly := {}

pomIncludeRepository := { _ => false }
publishMavenStyle := true
publishArtifact in Test := false

publishTo := sonatypePublishTo.value

// releaseCrossBuild := true // true if you cross-build the project for multiple Scala versions
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  // For non cross-build projects, use releaseStepCommand("publishSigned")
  // For cross-build projects, use releaseStepCommandAndRemaining("+publishSigned")
  releaseStepCommand("publishSigned"),
  setNextVersion,
  commitNextVersion,
  releaseStepCommand("sonatypeReleaseAll"),
  pushChanges
)