package csye7200

import org.apache.spark.sql.execution.columnar.MAP
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
object DataFramesBuilder{

  /**
    * Build parent edges DataFrame from file
    * @param edgesPath edges file path "nodes.dmp"
    * @param spark SparkSession
    * @return Try[DataFrame] of edges
    */
  def getEdgesParentDF(edgesPath : String, spark : SparkSession): Try[DataFrame] ={
    val edgesTuple = Try(spark.sparkContext.textFile(edgesPath)
      .map(_.split('|'))
      .map(line => (line.head.trim.toLong, line.tail.head.trim.toLong, "parent")))
    edgesTuple map(spark.createDataFrame(_).filter("_1 != _2").toDF("src", "dst", "relationship"))
  }

  /**
    * Build vertices DataFrame from file
    * @param verticesPath vertices file path "names.dmp"
    * @param spark SparkSession
    * @return Try[DataFrame] of vertices
    */
  def getVerticesDF(verticesPath: String, spark : SparkSession): Try[DataFrame] ={
    val verticesTuple = Try(spark.sparkContext.textFile(verticesPath)
      .map(_.split('|'))
      .map(line => (line.head.trim.toLong, line.tail.head.trim))
      .groupByKey()
      .map(line => (line._1, line._2.foldLeft(","){(b,a) => b + a + ","})))
    verticesTuple map(spark.createDataFrame(_).toDF("id", "name"))
  }

  /**
    * Build path to root DataFrame. But too slow to run.
    * @param edParentDF parent edges dataframe
    * @param spark SparkSession
    * @param maxLevel Max level to travel. -1 means travel all nodes.
    * @return DataFrame, id, pathToRoot String.
    */
  def buildPathToRootDF(edParentDF: DataFrame, spark : SparkSession, maxLevel: Int): DataFrame = {
    /**
      * tailrec to build pathToRoot Map
      * @param dst List of ids
      * @param level current level
      * @param maxLevel Max level to travel. -1 means travel all nodes.
      * @return List of ids for dst's Children.
      */
    @tailrec def bfsTraversal(dst: List[Long], level: Int, maxLevel: Int, p : Map[Long, String]): Map[Long, String] = {
      //nextLevelTuple List of (id, List(ids for children))
      val nextLevelTuple = dst.map(id => (id, edParentDF.filter(s"dst = $id").select("src").collect().toList.map(_.getLong(0))))
      val newList = for (t <- nextLevelTuple) yield{
        val parentId = t._1
        val parentPath = p.getOrElse(parentId,"/")
        for (id <- t._2) yield (id -> s"$parentPath$parentId/")
      }
      val newMap = newList.flatMap(_.map(i => i)).toMap[Long, String] ++ p
      val nextLevelIds = nextLevelTuple flatMap (i => i._2.map(j => j))
      if (nextLevelIds.isEmpty || (maxLevel != -1 && level >= maxLevel)) newMap
      else bfsTraversal(nextLevelIds,level + 1,maxLevel, newMap)
    }
    spark.createDataFrame(bfsTraversal(List(1L),1,maxLevel, Map(1L -> "/")).toSeq).toDF("id","path")
  }
}