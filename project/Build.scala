import sbt._
import Keys._
import sbtgitflow.ReleasePlugin._

object DfsDatastoresBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ releaseSettings ++ Seq(
    organization := "com.backtype",

    autoScalaLibrary := false,

    crossPaths := false,

    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

    libraryDependencies ++= Seq(
      "com.novocode" % "junit-interface" % "0.10-M2" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test" withSources(),
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test" withSources()
    ),

    resolvers ++= Seq(
      "Clojars" at "http://clojars.org/repo",
      "Concurrent Maven Repo" at "http://conjars.org/repo"
    ),

    parallelExecution in Test := false,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    pomExtra := (
      <url>https://github.com/nathanmarz/dfs-datastores</url>
      <licenses>
        <license>
          <name>Eclipse Public License</name>
          <url>http://www.eclipse.org/legal/epl-v10.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:nathanmarz/dfs-datastores.git</url>
        <connection>scm:git:git@github.com:nathanmarz/dfs-datastores.git</connection>
      </scm>
      <developers>
        <developer>
          <id>nathanmarz</id>
          <name>Nathan Marz</name>
          <url>http://twitter.com/nathanmarz</url>
        </developer>
        <developer>
          <id>sorenmacbeth</id>
          <name>Soren Macbeth</name>
          <url>http://twitter.com/sorenmacbeth</url>
        </developer>
        <developer>
          <id>sritchie</id>
          <name>Sam Ritchie</name>
          <url>http://twitter.com/sritchie</url>
        </developer>
      </developers>)
  )

  lazy val bundle = Project(
    id = "bundle",
    base = file("."),
    settings = sharedSettings
  ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(core, cascading)

  lazy val core = Project(
    id = "dfs-datastores",
    base = file("dfs-datastores"),
    settings = sharedSettings
  ).settings(
    name := "dfs-datastores",
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-log4j12" % "1.6.6",
      "jvyaml" % "jvyaml" % "1.0.0",
      "com.google.guava" % "guava" % "13.0",
      "org.apache.hadoop" % "hadoop-core" % "1.0.3"
    )
  )

  lazy val cascading = Project(
    id = "dfs-datastores-cascading",
    base = file("dfs-datastores-cascading"),
    settings = sharedSettings
  ).settings(
    name := "dfs-datastores-cascading",
    libraryDependencies ++= Seq(
      "cascading" % "cascading-core" % "2.0.7",
      "cascading" % "cascading-hadoop" % "2.0.7"
    )
  ).dependsOn(core)
}
