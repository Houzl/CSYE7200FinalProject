package csye7200

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
class
GraphFramesSearch{
  /**
    * Get full path from target vertices ID to Root(vid = 1)
    * @param edParentDF must be edParentDF
    * @param vid vertices ID
    * @param r result list
    * @return List of vertices
    */
  @tailrec final def gatPathToRoot(edParentDF: DataFrame, vid: Long, r : List[Long]): List[Long] = {
    val nextSrc = Try(edParentDF.filter(s"src = $vid").select("dst").head().getLong(0))
    nextSrc match {
      case Success(1L) => List(1L) ::: r
      case Success(n) => gatPathToRoot(edParentDF, n, List(n) ::: r)
      case Failure(_) => Nil
    }
  }

  /**
    * Check if target is subtree of src.
    * @param edParentDF edParentDF
    * @param targetVID target vertices ID
    * @param srcVID src vertices ID
    * @return Boolean
    */
  @tailrec final def isSubTree(edParentDF: DataFrame, targetVID: Long, srcVID: Long): Boolean = {
    //fi target vertices equals src vertices, return true.
    if (targetVID == srcVID) true;
    val nextTargetVID = Try(edParentDF.filter(s"src = $targetVID").select("dst").head().getLong(0))
    nextTargetVID match {
      case Success(`srcVID`) => true;
      case Success(1L) => false;
      case Failure(_) => false;
      case Success(n) => isSubTree(edParentDF, n, srcVID)
    }
  }

  /**
    * Find siblings vertices ids.
    * @param edParentDF edParentDF
    * @param vid parent vertices id
    * @return List of children vertices id, exclude itself
    */
  final def getSiblings(edParentDF: DataFrame, vid: Long): List[Long] ={
    val pVid = Try(edParentDF.filter(s"src = $vid").select("dst").head().getLong(0))
    pVid match {
      case Success(n) => {
        val siblings = Try(edParentDF.filter(s"dst = $n and src != $vid").select("src").rdd.map(r => r(0).asInstanceOf[Long]).collect().toList)
        siblings match {
          case Success(l) => l
          case Failure(_) => Nil
        }
      }
      case Failure(_) => Nil
    }
  }

  /**
    * Find children vertices ids.
    * @param edParentDF edParentDF
    * @param vid parent vertices id
    * @return List of children vertices id
    */
  final def getChildren(edParentDF: DataFrame, vid: Long): List[Long] ={
    val siblings = Try(edParentDF.filter(s"dst = $vid" ).select("src").rdd.map(r => r(0).asInstanceOf[Long]).collect().toList)
    siblings match {
      case Success(n) => n
      case Failure(_) => Nil
    }
  }
  /**
    * Get vid By name
    * @param veDF vertices DF
    * @param name String of name
    * @return get id return id, otherwise return -1. if get multiple ids, select one
    */
  final def findVidByName(veDF: DataFrame, name : String): Long = {
    if (name.isEmpty) -1L
    val vid = Try(veDF.filter(s"name like '%,$name,%'").select("id").head().getLong(0))
    vid match {
      case Success(n) => n
      case Failure(_) => -1L
    }
  }
}

object GraphFramesSearch extends App{
  // Get Spark Session
  val spark = SparkSession
    .builder()
    .appName("CSYE 7200 Final Project")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
  val edParentDF = spark.read.parquet(path + "edParentDF").persist(StorageLevel.MEMORY_ONLY)
  val edChildrenDF = spark.read.parquet(path + "edChildrenDF").persist(StorageLevel.MEMORY_ONLY)
  val veDF = spark.read.parquet(path + "veDF").persist(StorageLevel.MEMORY_ONLY)


  val GraphFramesSearch = new GraphFramesSearch
  println(GraphFramesSearch.gatPathToRoot(edParentDF,63221,List(63221)))
  //println(GraphFramesSearch.getSiblings(edParentDF,1))
  //println(GraphFramesSearch.findVidByName(veDF,"root"))
  //println(GraphFramesSearch.getSiblings(edParentDF,9606))

  // Create an identical GraphFrame.
  //val g = GraphFrame(ve, ed)
  // too slow.
  //  val paths = g.find("(a)-[e]->(b); (b)-[e2]->(c)")
  //    .filter("e.relationship = 'parent'")
  //    .filter("e2.relationship = 'children'")
  //    .filter("a.id = 9606")
  //    .filter("a.id != c.id")
  //  paths.show()


}
