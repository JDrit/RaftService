import AssemblyKeys._
import com.twitter.scrooge.ScroogeSBT._

name := "ScalaDB"

version := "1.0"

scalaVersion := "2.11.7"

assemblySettings

jarName in assembly := "scalaDB.jar"

resolvers += "Twitter" at "http://maven.twttr.com"

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.9.2",
  "com.twitter" %% "scrooge-core" % "4.2.0",
  "com.twitter" %% "finagle-thrift" % "6.30.0",
  "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0"
)


scroogeThriftSourceFolder in Compile <<= baseDirectory {
  base => base / "src/main/thrift"
}