import AssemblyKeys._
import com.twitter.scrooge.ScroogeSBT._
import sbt._
import Keys._

resolvers += "Twitter" at "http://maven.twttr.com"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7",
  organization := "edu.rit.csh",
  libraryDependencies ++= Seq(
    "org.apache.thrift" % "libthrift" % "0.9.2",
    "com.twitter" %% "scrooge-core" % "4.2.0",
    "com.twitter" %% "finagle-thrift" % "6.30.0",
    "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0",
    "ch.qos.logback" % "logback-classic" % "1.1.2"
  )
)

lazy val root = Project("main", file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "ScalaDB",
    assemblySettings,
    jarName in assembly := "scalaDB.jar",
    scroogeThriftSourceFolder in Compile <<= baseDirectory {
      base => base / "src/main/thrift"
    })
