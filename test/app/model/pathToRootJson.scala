package model

import model.{DataFramesBuilder, IndexDataFramesSearch}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
  * Created by mali on 12/8/16.
  */

object pathToRootJson{

  case class Node(ID: Long, Name: String)


  def search(spark: SparkSession, nodeID:Long) = {
    val path = "/Users/mali/Downloads/taxdmp/"
    val edgesPath = path + "nodes.dmp"
    val verticesPath = path + "names.dmp"

    implicit val nodesWrites: Writes[Node] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String]
      ) (unlift(Node.unapply))


    val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
    val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
    val df = edParentDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
    val bv = DataFramesBuilder.buildPathToRootDF(df, spark, 3)
    //  val edParentDF = spark.read.parquet(path + "edParentDF").persist(StorageLevel.MEMORY_ONLY).cache()
    //  val veDF = spark.read.parquet(path + "veDF").persist(StorageLevel.MEMORY_ONLY).cache()
    val indexDF = spark.read.parquet(path + "pathToRootDF").persist(StorageLevel.MEMORY_ONLY).cache()
    println(IndexDataFramesSearch.getPathToRoot(indexDF, nodeID, List(nodeID)))
    println("success！！！")
    val result = IndexDataFramesSearch.getPathToRoot(indexDF, nodeID,List(nodeID))



    val ptr = result.map(i => Node(i, DataFramesSearch.findVNameByID(indexDF, i)))
    println(ptr)
    val json = Json.toJson(ptr)
    json

  }
}
