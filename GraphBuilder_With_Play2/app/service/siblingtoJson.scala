package service
import service.IndexDataFramesSearch
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Writes}

/**
  * Created by mali on 12/1/16.
  * The object contains json parsing method which serves to the searching request
  */

object siblingtoJson{

  /**
    * Build Node case class to form JSON structure
    */
  case class Node(ID: Long, Name: String)

  /**
    * Build search method to search list of path nodes and return in json format
    * @param spark current Sparksession
    * @param nodeID The given node which would be provided by user
    */
  def search(spark: SparkSession, nodeID:Long) = {
    val path = "/Users/mali/Downloads/taxdmp/"
    val edgesPath = path + "nodes.dmp"
    val verticesPath = path + "names.dmp"

    /**
      * Implicit Json conversion function
      * use play.api.libs.json._
      */
    implicit val nodesWrites: Writes[Node] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String]
      ) (unlift(Node.unapply))

    /**
      * Load data and search
      */

    val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
    val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
    val vf = veDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
    val df = edParentDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
    val bv = DataFramesBuilder.buildPathToRootDF(df, spark, 3)

    val indexDF = spark.read.parquet(path + "pathToRootDF").persist(StorageLevel.MEMORY_ONLY).cache()
    val result = IndexDataFramesSearch.getSiblings(bv, nodeID)
    println(result)
    val siblings = result.map(i => Node(i, DataFramesSearch.findVNameByID(vf, i).replace(","," ")))
    println(siblings)
    val json = Json.toJson(siblings)
    json

  }

}