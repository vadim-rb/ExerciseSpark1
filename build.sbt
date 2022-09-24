ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "twtest2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.1" % "provided"
  )
