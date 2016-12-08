package csye7200

import org.graphframes.GraphFrame

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/21/2016.
  */
object GraphFramesSearch {
  /**
    * Check if target is subtree of src.
    * @param graph
    * @param targetVID target vertices ID
    * @param srcVID src vertices ID
    * @return Boolean
    */
  @tailrec final def isSubTree(graph: GraphFrame, targetVID: Long, srcVID: Long): Boolean = {
    //if target vertices equals src vertices, return true.
    if (targetVID == srcVID) true
    else {
      val nextTargetVID = Try(graph.find("(src)-[]->(dts)").filter(s"src.id = $targetVID").select("dts.id").head().getLong(0))
      nextTargetVID match {
        case Success(`srcVID`) => true;
        case Success(1L) => false;
        case Failure(_) => false;
        case Success(n) => isSubTree(graph, n, srcVID)
      }
    }
  }

  /**
    * Get full path from target vertices ID to Root(vid = 1)
    * @param graph
    * @param vid vertices ID
    * @param r result list
    * @return List of vertices
    */
  //TODO Change List[Long] to RDD[LONG]
  @tailrec final def getPathToRoot(graph: GraphFrame, vid: Long, r : List[Long]): List[Long] = {
    val nextSrc = Try(graph.find("(src)-[]->(dts)").filter(s"src.id = $vid").select("dts.id").head().getLong(0))
    nextSrc match {
      case Success(1L) => List(1L) ::: r
      case Success(n) => getPathToRoot(graph, n, List(n) ::: r)
      case Failure(_) => Nil
    }
  }

  /**
    * Find siblings vertices ids.
    * @param graph
    * @param vid parent vertices id
    * @return List of children vertices id, exclude itself
    */
  final def getSiblings(graph: GraphFrame, vid: Long): List[Long] ={
    val rows = graph.find("(src)-[]->(a); (dts)-[]->(a)").filter(s"src.id = $vid").filter(s"dts.id != $vid").select("dts.id").collect().toList
    for (row <- rows) yield row.getLong(0)
  }

  /**
    * Find children vertices ids.
    * @param graph
    * @param vid parent vertices id
    * @return List of children vertices id
    */
  final def getChildren(graph: GraphFrame, vid: Long): List[Long] ={
    val rows = graph.find("(dts)-[]->(src)").filter(s"src.id = $vid").select("dts.id").collect().toList
    for (row <- rows) yield row.getLong(0)
  }
}
