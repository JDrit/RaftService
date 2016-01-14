import AssemblyKeys._
import com.twitter.scrooge.ScroogeSBT._
import com.twitter.scrooge.ScroogeSBT.autoImport._
import sbt._
import sbt.Keys._
import sbtassembly.Plugin._


val compilerVersion = "2.11.7"

lazy val commonSettings = Seq(
  resolvers += Resolver.sonatypeRepo("snapshots"),
  resolvers += Resolver.sonatypeRepo("releases"),
  resolvers ++= Seq(
    "sonatype" at "https://oss.sonatype.org/content/groups/public",
    "twttr" at "https://maven.twttr.com"),
  version := "0.1",
  scalaVersion := compilerVersion,
  organization := "edu.rit.csh.jdb",
  scalacOptions ++= Seq("-Xplugin-require:scalaxy-streams", "-optimise", "-Yclosure-elim", "-Yinline"),
  autoCompilerPlugins := true,
  addCompilerPlugin("com.nativelibs4java" %% "scalaxy-streams" % "0.3.4"),
  libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test")

/**
 * The Thrift service declarations used for the raft implementation
 */
lazy val thrift = project.in(file("common"))
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

lazy val Benchmark = config("bench") extend Test

lazy val serialization = project.in(file("serialization"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-log4j12" % "1.7.13",
      "org.scala-lang" % "scala-reflect" % compilerVersion,
      "org.apache.thrift" % "libthrift" % "0.9.2",
      "com.twitter" %% "scrooge-core" % "4.3.0",
      "com.twitter" %% "finagle-thrift" % "6.31.0",
      "com.storm-enroute" %% "scalameter" % "0.7"),
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    logBuffered := false,
    parallelExecution in Benchmark := false,
    libraryDependencies := { CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, scalaMajor)) if scalaMajor >= 11 => libraryDependencies.value
      case Some((2, 10)) => libraryDependencies.value ++ Seq(
        compilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
        "org.scalamacros" %% "quasiquotes" % "2.0.0" cross CrossVersion.binary)
      }
    },
    scroogeThriftSourceFolder in Compile <<= baseDirectory {
      base => base / "src/bench/thrift"
    })
  .configs(Benchmark)
  .settings(inConfig(Benchmark)(Defaults.testSettings): _*)


/**
 * Actual Raft implementation. All internal raft logic is kept in this package.
 */
lazy val raft = project.in(file("raft"))
  .settings(commonSettings: _*)
  .settings(
    name := "raft",
    libraryDependencies ++= Seq(
      "com.twitter" %% "twitter-server" % "1.16.0",
      "com.twitter" %% "finagle-stats" % "6.31.0"))
  .dependsOn(thrift)
  .dependsOn(serialization)

/**
 * Example database siting on top of the raft protocol
 */
lazy val server = project.in(file("server"))
  .settings(commonSettings: _*)
  .settings(
    name := "server",
    assemblySettings,
    jarName in assembly := "jdb.jar",
    excludedJars in assembly := (fullClasspath in assembly).value filter {_.data.getName == "asm-3.1.jar"},
    libraryDependencies ++= Seq(
      "com.github.finagle" %% "finch-core" % "0.9.2",
      "com.github.finagle" %% "finch-circe" % "0.9.2",
      "com.twitter" %% "twitter-server" % "1.16.0",
      "com.twitter" %% "finagle-stats" % "6.31.0",
      "io.circe" %% "circe-core" % "0.2.1",
      "io.circe" %% "circe-generic" % "0.2.1",
      "io.circe" %% "circe-parse" % "0.2.1"))
  .dependsOn(raft)

/**
 * Admin interface to the raft cluster. Used for things like server stats and
 * configuration changes
 */
lazy val admin = project.in(file("admin"))
  .settings(commonSettings: _*)
  .settings(
    name := "admin",
    assemblySettings,
    jarName in assembly := "admin.jar")
  .dependsOn(thrift)