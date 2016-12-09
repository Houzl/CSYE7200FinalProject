import play.PlayScala

name := """scala-dci"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
    "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test,
    "org.apache.spark" % "spark-sql_2.11" % "2.0.2",
    "org.apache.spark" % "spark-core_2.11" % "2.0.2",
    "org.apache.spark" % "spark-graphx_2.11" % "2.0.2",
    "graphframes" % "graphframes" % "0.3.0-spark2.0-s_2.11",
  "org.specs2" % "specs2_2.11" % "3.7",
    jdbc,
  cache,
  ws).map(_.exclude("org.slf4j", "slf4j-log4j12"))


fork in run := true