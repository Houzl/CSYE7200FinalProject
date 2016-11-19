name := "GraphBuilder"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Central Repository" at "http://repo1.maven.org/maven2/"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Spark Packages Repository" at "https://dl.bintray.com/spark-packages/maven/"

//from http://repo1.maven.org/maven2/
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.0.2"

// https://mvnrepository.com/artifact/graphframes/graphframes
libraryDependencies += "graphframes" % "graphframes" % "0.3.0-spark2.0-s_2.11"