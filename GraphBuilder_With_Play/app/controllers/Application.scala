package controllers

import service._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import play.api.mvc._
import play.api.libs.json.Json._
import play.api.libs.json._
import service._
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Writes}

/**
  * Created by mali on 12/1/16.
  * The Controller handles the GET request which is mapped in routes
  */
object Application extends Controller {



  def index = Action { implicit request =>
    val spark = SparkSession
      .builder()
      .appName("CSYE 7200 Final Project2")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    case class Node(ID: Long, Name: String)
    case class fatherNode(ID:Long, Name:String, children:List[Node])

    implicit val nodesWrites: Writes[Node] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String]
      )(unlift(Node.unapply))

    implicit val fatherNodeWrites: Writes[fatherNode] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String] and
        (JsPath \ "children").write[List[Node]]
      )(unlift(fatherNode.unapply))


    val result = childtoJson.search(spark,1)
    Ok(result)

  }
  
  def searchChild(nodeID:String) = Action{
    implicit request =>

      val spark = SparkSession
        .builder()
        .appName("CSYE 7200 Final Project2")
        .master("local[2]")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

      case class Node(ID: Long, Name: String)
      case class fatherNode(ID:Long, Name:String, children:List[Node])

      implicit val nodesWrites: Writes[Node] = (
        (JsPath \ "ID").write[Long] and
          (JsPath \ "Name").write[String]
        )(unlift(Node.unapply))

      implicit val fatherNodeWrites: Writes[fatherNode] = (
        (JsPath \ "ID").write[Long] and
          (JsPath \ "Name").write[String] and
          (JsPath \ "children").write[List[Node]]
        )(unlift(fatherNode.unapply))

      val result = childtoJson.search(spark,nodeID.toLong)
      Ok(result)
  }

  def searchPtoR(nodeID:String) = Action{
    implicit request =>

      val spark = SparkSession
        .builder()
        .appName("CSYE 7200 Final Project2")
        .master("local[2]")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

      case class Node(ID: Long, Name: String)
      case class fatherNode(ID:Long, Name:String, children:List[Node])

      implicit val nodesWrites: Writes[Node] = (
        (JsPath \ "ID").write[Long] and
          (JsPath \ "Name").write[String]
        )(unlift(Node.unapply))

      implicit val fatherNodeWrites: Writes[fatherNode] = (
        (JsPath \ "ID").write[Long] and
          (JsPath \ "Name").write[String] and
          (JsPath \ "children").write[List[Node]]
        )(unlift(fatherNode.unapply))

      val result = pathToRootJson.search(spark,nodeID.toLong)
      Ok(result)
  }

  def searchsibling(nodeID:String) = Action{
    val spark = SparkSession
      .builder()
      .appName("CSYE 7200 Final Project2")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    case class Node(ID: Long, Name: String)

    implicit val nodesWrites: Writes[Node] = (
      (JsPath \ "ID").write[Long] and
        (JsPath \ "Name").write[String]
      )(unlift(Node.unapply))

    val result = siblingtoJson.search(spark,nodeID.toLong)
    Ok(result)

  }

}
