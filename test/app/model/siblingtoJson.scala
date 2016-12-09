package model
import model.{DataFramesBuilder, DataFramesSearch, IndexDataFramesSearch}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Writes}

/**
  * Created by mali on 12/8/16.
  */

object siblingtoJson{

  case class Node(ID: Long, Name: String)
  case class fatherNode(ID:Long, Name:String, children:List[Node])



  def search(spark: SparkSession, nodeID:Long) = {
    val path = "/Users/mali/Downloads/taxdmp/"
    val edgesPath = path + "nodes.dmp"
    val verticesPath = path + "names.dmp"

    implicit val nodesWrites: Writes[Node] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String]
      ) (unlift(Node.unapply))

    implicit val fatherNodeWrites: Writes[fatherNode] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String] and
        (JsPath \ "children").write[List[Node]]
      ) (unlift(fatherNode.unapply))

    val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
    val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
    val df = edParentDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
    val bv = DataFramesBuilder.buildPathToRootDF(df, spark, 3)
    //  val edParentDF = spark.read.parquet(path + "edParentDF").persist(StorageLevel.MEMORY_ONLY).cache()
    //  val veDF = spark.read.parquet(path + "veDF").persist(StorageLevel.MEMORY_ONLY).cache()

    val indexDF = spark.read.parquet(path + "pathToRootDF").persist(StorageLevel.MEMORY_ONLY).cache()
    val result = IndexDataFramesSearch.getSiblings(bv, nodeID)
    println(result)
    val nodeFather = new Node(nodeID, DataFramesSearch.findVNameByID(df, nodeID))
    println(nodeFather.ID + " : " + nodeFather.Name)
    val siblings = result.map(i => Node(i, DataFramesSearch.findVNameByID(df, i)))
    println(siblings)
    val tree = new fatherNode(nodeID, DataFramesSearch.findVNameByID(df, nodeID), siblings)
    val json = Json.toJson(tree)
    json

  }

}