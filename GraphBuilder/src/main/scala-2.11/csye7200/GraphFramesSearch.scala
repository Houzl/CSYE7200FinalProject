package csye7200

import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
object GraphFramesSearch{
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
      case Success(n) => edParentDF.filter(s"dst = $n and src != $vid").select("src").rdd.map(r => r(0).asInstanceOf[Long]).collect().toList
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
    edParentDF.filter(s"dst = $vid" ).select("src").rdd.map(r => r(0).asInstanceOf[Long]).collect().toList
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
