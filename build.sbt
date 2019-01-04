name := """play-scala-seed"""
organization := "com.example"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"


libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies ++= Seq(
  guice,
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test,
  jdbc,
  "mysql" % "mysql-connector-java" % "5.1.46",
  "com.typesafe.play" %% "anorm" % "2.5.3"
)
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.9"



libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % "10.1.5",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.1.5" % Test
)

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.0"

libraryDependencies += "org.apache.hive" % "hive-exec" % "1.2.1" excludeAll
  ExclusionRule(organization = "org.pentaho")
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
libraryDependencies += "org.apache.hive" % "hive-service" % "1.2.1"
libraryDependencies += "org.apache.hive" % "hive-cli" % "1.2.1"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.1.0"

libraryDependencies += "org.apache.hive" % "hive-exec" % "1.2.1"
libraryDependencies += "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test

resolvers += Resolver.mavenLocal
resolvers += "Cascading repo" at "http://conjars.org/repo"

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.example.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.example.binders._"
