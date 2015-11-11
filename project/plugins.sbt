logLevel := Level.Warn

resolvers += "sonatype" at "https://oss.sonatype.org/content/groups/public"

addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "3.18.1")

addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.11.2")