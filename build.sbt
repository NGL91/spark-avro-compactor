lazy val buildSettings = Seq(
  name := "spark-avro-compactor",
  organization := "ie.ianduffy"
)

lazy val sparkVersion = "2.4.0"
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

lazy val baseSettings = Seq(
  scalaVersion := "2.11.11",
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlog-reflective-calls",
    "-Xlint"
  ),
  javacOptions in Compile ++= Seq(
    "-source", "1.8"
    , "-target", "1.8"
    , "-Xlint:unchecked"
    , "-Xlint:deprecation"
  ),

  fork in run := true,
  fork in Test := true,

  assemblyJarName := name.value + "_" + version.value + ".jar",
  assemblyMergeStrategy := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  parallelExecution in Test := false,
  resolvers += "confluent" at "http://packages.confluent.io/maven",
  libraryDependencies ++= {
    Seq(
      ("org.apache.hadoop" % "hadoop-aws" % "2.7.3") excludeAll(
        ExclusionRule(organization = "commons-beanutils")
        ),
      "ch.qos.logback" % "logback-classic" % "1.1.3",
      "com.typesafe" % "config" % "1.2.1",
      "com.github.scopt" %% "scopt" % "3.7.0",
//      "com.amazonaws" % "aws-java-sdk-core" % "1.10.6",
      "org.apache.spark" %% "spark-avro" % sparkVersion,
      "org.scalatest" %% "scalatest" % "3.0.4" % Test,
      "io.confluent"    % "kafka-schema-registry-client" % "3.3.0",
      "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % Test,
      "org.mockito" % "mockito-core" % "2.7.6" % Test
    ).map(_.exclude("org.slf4j", "slf4j-log4j12")) ++ sparkDependencies.map(dependency => dependency % Provided)
  },
  git.useGitDescribe := true
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"


lazy val root = (project in file("."))
  .enablePlugins(GitVersioning, JavaAppPackaging)
  .settings(baseSettings)
  .settings(buildSettings)
