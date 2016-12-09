package service

import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
object IndexDataFramesSearch{
  /**
    * Get full path from target vertices ID to Root(vid = 1)
    * @param pathToRootDF must be pathToRootDF
    * @param vid vertices ID
    * @param r result list
    * @return List of vertices
    */
  //TODO Change List[Long] to RDD[LONG]
  final def getPathToRoot(pathToRootDF: DataFrame, vid: Long, r : List[Long]): List[Long] = {
    val path = Try(pathToRootDF.filter(s"id = $vid").select("path").head().getString(0))
    path match {
      case Success("/") => Nil
      case Success("") => Nil
      case Success(n) => n.split("/").filterNot(s => s.trim.isEmpty).toList.map(_.toLong) ::: r
      case Failure(_) => Nil
    }
  }

  /**
    * Check if target is subtree of src.
    * @param pathToRootDF pathToRootDF
    * @param targetVID target vertices ID
    * @param srcVID src vertices ID
    * @return Boolean
    */
  final def isSubTree(pathToRootDF: DataFrame, targetVID: Long, srcVID: Long): Boolean = {
    //fi target vertices equals src vertices, return true.
    if (targetVID == srcVID) true
    else{
      val pathTarget = pathToRootDF.filter(s"id = $targetVID").select("path").head().getString(0)
      pathTarget.indexOf(s"/$srcVID/") match {
        case -1 => false
        case _ => true
      }
    }
  }

  /**
    * Find siblings vertices ids.
    * @param pathToRootDF pathToRootDF
    * @param vid target vertices id
    * @return List of getSiblings vertices id, exclude itself.
    */
  final def getSiblings(pathToRootDF: DataFrame, vid: Long): List[Long] ={
    val path = Try(pathToRootDF.filter(s"id = $vid").select("path").head().getString(0))
    path match {
      case Success(p) => pathToRootDF.filter(s"path = '$p' and id != $vid").select("id").collect().toList.map(row => row.getLong(0))
      case Failure(_) => Nil
    }
  }

  /**
    * Find children vertices ids.
    * @param pathToRootDF pathToRootDF
    * @param vid parent vertices id
    * @return List of children vertices id
    */
  final def getChildren(pathToRootDF: DataFrame, vid: Long): List[Long] ={
    val path = Try(pathToRootDF.filter(s"id = $vid").select("path").head().getString(0))
    path match {
      case Success(p) => pathToRootDF.filter(s"path = '$p$vid/'").select("id").collect().toList.map(row => row.getLong(0))
      case Failure(_) => Nil
    }
  }
}