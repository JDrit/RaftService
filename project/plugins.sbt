logLevel := Level.Warn

resolvers ++= Seq(
  "sonatype" at "https://oss.sonatype.org/content/groups/public",
  "Twitter" at "http://maven.twttr.com")

addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "3.18.1")

addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.11.2")