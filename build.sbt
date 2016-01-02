import AssemblyKeys._
import com.twitter.scrooge.ScroogeSBT._
import com.twitter.scrooge.ScroogeSBT.autoImport._
import sbt._
import sbt.Keys._
import sbtassembly.Plugin._

lazy val commonSettings = Seq(
  resolvers ++= Seq(
    "sonatype" at "https://oss.sonatype.org/content/groups/public",
    "twttr" at "https://maven.twttr.com"),
  version := "1.0",
  scalaVersion := "2.11.7",
  organization := "edu.rit.csh.jdb"
)

/**
 * The Thrift service declarations used for the raft implementation
 */
lazy val common = project.in(file("common"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.9.2",
      "com.twitter" %% "scrooge-core" % "4.3.0",
      "com.twitter" %% "finagle-thrift" % "6.31.0"
    ),
    scroogeThriftSourceFolder in Compile <<= baseDirectory {
      base => base / "src/main/thrift"
    })

/**
 * Actual Raft implementation. All internal raft logic is kept in this package.
 */
lazy val raft = project.in(file("raft"))
  .settings(commonSettings: _*)
  .settings(
    name := "raft",
    libraryDependencies ++= Seq(
      "com.twitter" % "twitter-server_2.11" % "1.16.0",
      "com.twitter" % "finagle-stats_2.11" % "6.31.0"
    )
  ).dependsOn(common)

/**
 * Example database siting on top of the raft protocol
 */
lazy val server = project.in(file("server"))
  .settings(commonSettings: _*)
  .settings(
    name := "server",
    assemblySettings,
    jarName in assembly := "jdb.jar",
    libraryDependencies ++= Seq(
      "com.github.finagle" % "finch-core_2.11" % "0.9.2",
      "com.github.finagle" % "finch-circe_2.11" % "0.9.2",
      "com.twitter" % "twitter-server_2.11" % "1.16.0",
      "com.twitter" % "finagle-stats_2.11" % "6.31.0",
      "io.circe" %% "circe-core" % "0.2.1",
      "io.circe" %% "circe-generic" % "0.2.1",
      "io.circe" %% "circe-parse" % "0.2.1"
    )
  ).dependsOn(raft)

/**
 * Admin interface to the raft cluster. Used for things like server stats and
 * configuration changes
 */
lazy val admin = project.in(file("admin"))
  .settings(commonSettings: _*)
  .settings(
    name := "admin",
    assemblySettings,
    jarName in assembly := "admin.jar"
  ).dependsOn(common)