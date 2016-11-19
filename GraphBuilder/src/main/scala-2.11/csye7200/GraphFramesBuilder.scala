package csye7200

import java.io.IOException

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
case class GraphFramesBuilder(verticesPath: String, edgesPath : String, spark : SparkSession) {

  def getEdgesParentDF: DataFrame ={
    val edgesTuple = Try(spark.sparkContext.textFile(edgesPath)
      .map(_.split('|'))
      .map(line => (line.head.trim.toLong, line.tail.head.trim.toLong, "parent")))
    edgesTuple match {
      case Success(n) => spark.createDataFrame(n).filter("_1 != _2").toDF("src", "dst", "relationship")
      case Failure(x) => throw new IOException
    }
  }

  def getEdgesChildrenDF: DataFrame ={
    val edgesTuple = Try(spark.sparkContext.textFile(edgesPath)
      .map(_.split('|'))
      .map(line => (line.tail.head.trim.toLong, line.head.trim.toLong, "children")))
    edgesTuple match {
      case Success(n) => spark.createDataFrame(n).filter("_1 != _2").toDF("src", "dst", "relationship")
      case Failure(_) => throw new IOException
    }
  }

  def getEdgesDF: DataFrame = getEdgesParentDF union getEdgesChildrenDF

  def getVerticesDF: DataFrame ={
    val verticesTuple = spark.sparkContext.textFile(verticesPath)
      .map(_.split('|'))
      .map(line => (line.head.trim.toLong, line.tail.head.trim))
      .groupByKey()
      .map(line => (line._1, line._2.foldLeft(","){(b,a) => b + a + ","}))
    spark.createDataFrame(verticesTuple).toDF("id", "name")
  }
}

object GraphFramesBuilder extends App{
  // Get Spark Session
  val spark = SparkSession
    .builder()
    .appName("CSYE 7200 Final Project")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
  val edgesPath = path + "nodes.dmp"
  val verticesPath = path + "names.dmp"
  val GraphFramesBuilder = new GraphFramesBuilder(verticesPath, edgesPath, spark);
  val edParentDF = GraphFramesBuilder.getEdgesParentDF
  val edChildrenDF = GraphFramesBuilder.getEdgesChildrenDF
  val veDF = GraphFramesBuilder.getVerticesDF

  edParentDF.write.parquet(path + "edParentDF")
  edChildrenDF.write.parquet(path + "edChildrenDF")
  veDF.write.parquet(path + "veDF")
}