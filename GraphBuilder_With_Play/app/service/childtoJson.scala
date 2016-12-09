package service
import service.IndexDataFramesSearch
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
  * Created by mali on 12/1/16.
  * The object contains json parsing method which serves to the searching request
  */

  object childtoJson {
  /**
  * Build Node case class to form JSON structure
  */
  case class Node(ID: Long, Name: String)
  case class fatherNode(ID:Long, Name:String, children:List[Node])


  /**
    * Build search method to search list of child nodes and return in json format
    * @param spark current Sparksession
    * @param nodeID The father node which would be provided by user
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
      )(unlift(Node.unapply))

    implicit val fatherNodeWrites: Writes[fatherNode] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String] and
        (JsPath \ "children").write[List[Node]]
      )(unlift(fatherNode.unapply))
    /**
      * Load data and search
      */
  val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
  val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
  val df = edParentDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
    val vf = veDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
  val bv = DataFramesBuilder.buildPathToRootDF(df, spark, 3)
  val indexDF = spark.read.parquet(path + "pathToRootDF").persist(StorageLevel.MEMORY_ONLY).cache()
  println(IndexDataFramesSearch.getChildren(indexDF, nodeID))
  println("success！！！")
  val result = IndexDataFramesSearch.getChildren(indexDF, nodeID)
    val nodeFather = new Node(nodeID,DataFramesSearch.findVNameByID(vf,nodeID).replace(","," "))
    println(nodeFather.ID+" : "+nodeFather.Name)
    val childs = result.map(i => Node(i,DataFramesSearch.findVNameByID(vf,i).replace(","," ")))
    println(childs)
    val tree = new fatherNode(nodeID,DataFramesSearch.findVNameByID(vf,nodeID).replace(","," "),childs)
    val json = Json.toJson(tree)
    json

}
}