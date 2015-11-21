import AssemblyKeys._
import com.twitter.scrooge.ScroogeSBT._
import com.twitter.scrooge.ScroogeSBT.autoImport._
import sbt._
import sbt.Keys._
import sbtassembly.Plugin._

resolvers += "Twitter" at "http://maven.twttr.com"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7",
  organization := "edu.rit.csh",
  libraryDependencies ++= Seq(
    "org.apache.thrift" % "libthrift" % "0.9.2",
    "com.twitter" %% "scrooge-core" % "4.2.0",
    "com.twitter" %% "finagle-thrift" % "6.30.0"
  )
)

lazy val common = project.in(file("common"))
  .settings(commonSettings: _*)
  .settings(scroogeThriftSourceFolder in Compile <<= baseDirectory {
    base => base / "src/main/thrift"
  })

lazy val server = project.in(file("server"))
  .settings(commonSettings: _*)
  .settings(
    name := "ScalaDB",
    assemblySettings,
    jarName in assembly := "scalaDB.jar",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0",
      "ch.qos.logback" % "logback-classic" % "1.1.2"
    ), scroogeThriftSourceFolder in Compile <<= baseDirectory {
      base => base / "src/main/thrift"
    }
  ).dependsOn(common)

lazy val client = project.in(file("client"))
  .settings(commonSettings: _*)
  .settings(
    name := "client",
    assemblySettings,
    jarName in assembly := "client.jar"
  ).dependsOn(common)


