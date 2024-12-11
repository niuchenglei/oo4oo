import sbt.Keys._

ThisBuild / organization := "mleap-oplite"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version      := "0.0.1"

val sparkVersion = "3.2.2"
val mleapVersion = "0.23.0"

val mainClassName = "Main"

Compile / run / mainClass          := Some(mainClassName) // for the main 'sbt run' task
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

val dependencies = Seq(
  "org.apache.spark" %% "spark-core"            % sparkVersion,
  "org.apache.spark" %% "spark-mllib"           % sparkVersion,
  "ml.combust.mleap" %% "mleap-runtime"         % mleapVersion,
  "ml.combust.mleap" %% "mleap-spark-base"      % mleapVersion,
  "ml.combust.mleap" %% "mleap-spark-extension" % mleapVersion,
  "org.scalatest"    %% "scalatest-funspec"     % "3.2.14" % "test",
  "org.slf4j"         % "slf4j-log4j12"         % "1.7.25",
  "ch.qos.logback"    % "logback-classic"       % "1.2.3",
  "ch.qos.logback"    % "logback-core"          % "1.+",
  "io.jvm.uuid"      %% "scala-uuid"            % "0.3.1"
)

lazy val root = (project in file("."))
  .settings(
    name       := "mleap-oo4oo",
    libraryDependencies ++= dependencies,
    run / fork := true,
    run / javaOptions ++= Seq("-Dspark.master=local[*]")
  )
