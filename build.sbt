cancelable in Global := true

name := "movielens-events"

version := "0.2.0"

scalaVersion := "2.12.15"

organization := "com.advandata"

ThisBuild / organizationName := "advandata"

ThisBuild / licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

resolvers ++= Seq(
  Classpaths.typesafeReleases,
  "Confluent Maven Repository" at "https://packages.confluent.io/maven/"
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "." + artifact.extension
}

scalacOptions += "-Ypartial-unification"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"

libraryDependencies ++= Seq(
  "io.github.moleike" %% "kafka-streams-avro-scala" % "0.2.5"
)

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.4.0",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.4.0"
)

assembly / assemblyJarName := s"${name.value}.jar"

assembly / assemblyMergeStrategy := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}