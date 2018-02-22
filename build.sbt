name := "akka-stream-message-hub"

version := "0.1"

organization := "com.typesafe.akka"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.4",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "com.typesafe.akka" %% "akka-http" % "10.0.11",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.11",
  "org.apache.httpcomponents" % "httpclient" % "4.5.4",

  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)
