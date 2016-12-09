package csye7200

import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
object DataFramesSearch{
  /**
    * Get full path from target vertices ID to Root(vid = 1)
    * @param edParentDF must be edParentDF
    * @param vid vertices ID
    * @param r result list
    * @return List of vertices
    */
  //TODO Change List[Long] to RDD[LONG]
  @tailrec final def getPathToRoot(edParentDF: DataFrame, vid: Long, r : List[Long]): List[Long] = {
    val nextSrc = Try(edParentDF.filter(s"src = $vid").select("dst").head().getLong(0))
    nextSrc match {
      case Success(1L) => List(1L) ::: r
      case Success(n) => getPathToRoot(edParentDF, n, List(n) ::: r)
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
    if (targetVID == srcVID) true
    else{
      val nextTargetVID = Try(edParentDF.filter(s"src = $targetVID").select("dst").head().getLong(0))
      nextTargetVID match {
        case Success(`srcVID`) => true;
        case Success(1L) => false;
        case Failure(_) => false;
        case Success(n) => isSubTree(edParentDF, n, srcVID)
      }
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
      case Success(n) => edParentDF.filter(s"dst = $n and src != $vid").select("src").collect().toList.map(row => row.getLong(0))
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
    edParentDF.filter(s"dst = $vid" ).select("src").collect().toList.map(row => row.getLong(0))
  }
  /**
    * Get vid By name
    * @param veDF vertices DF
    * @param name String of name
    * @return get id return id, otherwise return -1. if get multiple ids, select one
    */
  final def findVidByName(veDF: DataFrame, name : String): Long = {
    if (name.isEmpty) -1L
    else{
      val vid = Try(veDF.filter(s"name like '%,$name,%'").select("id").head().getLong(0))
      vid match {
        case Success(n) => n
        case Failure(_) => -1L
      }
    }
  }
  /**
    * Get name By id
    * @param veDF vertices DF
    * @param vid Long
    * @return if success return name, otherwise return ""
    */
  final def findNameByVID(veDF: DataFrame, vid : Long): String = {
    if (vid < 1L) ""
    else{
      val names = Try(veDF.filter(s"id = $vid").select("name").head().getString(0))
      names match {
        case Success(n) => n
        case Failure(_) => ""
      }
    }
  }
}
