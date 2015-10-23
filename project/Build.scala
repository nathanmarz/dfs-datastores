import sbt._
import Keys._

object Build extends Build {
  val ScalaVersion = "2.10.4"

  val sharedSettings = Project.defaultSettings ++ Seq(
    organization := "com.backtype",

    crossPaths := false,

    javacOptions ++= Seq("-source", "1.6", "-target", "1.7"),
    javacOptions in doc := Seq("-source", "1.7"),

    libraryDependencies ++= Seq(
      "com.novocode" % "junit-interface" % "0.11" % "test",
      // To silence warning in test logs. Library should depend only on the API.
      "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "test"
    ),

    resolvers ++= Seq(
      "Clojars" at "http://clojars.org/repo",
      "Concurrent Maven Repo" at "http://conjars.org/repo",
      "Twttr Maven Repo"  at "http://maven.twttr.com/",
      "Cloudera Repo"  at "https://repository.cloudera.com/artifactory/cloudera-repos"
    ),

    parallelExecution in Test := false,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    scalaVersion := ScalaVersion,

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in (Compile, packageDoc) := false,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version {
      (v: String) =>
        if (v.trim.endsWith("SNAPSHOT"))
          Some("Indix Snapshot Artifactory" at "http://artifacts.indix.tv:8081/artifactory/libs-snapshot-local")
        else
          Some("Indix Release Artifactory" at "http://artifacts.indix.tv:8081/artifactory/libs-release-local")
    },

    //publishTo := Some(Resolver.file("file",  new File( Path.userHome.absolutePath + "/mvn_repo/repository/releases" )) ),

    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),

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
//    test := { },
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
      "org.slf4j" % "slf4j-api" % "1.6.6",
      "jvyaml" % "jvyaml" % "1.0.0",
      "com.google.guava" % "guava" % "13.0",
      "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.2.1" % "provided",
      "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.2.1" % "test",
      "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.15",
      "org.scalatest" %% "scalatest" % "2.2.0" % "test",
      "models" %% "models" % "2.0.175",
      "org.apache.thrift" % "libthrift" % "0.8.0",
      "com.twitter" % "scalding-args_2.10" % "0.15.0",
      "org.scalaj" % "scalaj-time_2.9.2" % "0.6"
    ).map(_.exclude("commons-daemon", "commons-daemon"))
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

