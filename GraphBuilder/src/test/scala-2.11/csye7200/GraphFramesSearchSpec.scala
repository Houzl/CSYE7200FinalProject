package csye7200

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by houzl on 11/18/2016.
  */
class GraphFramesSearchSpec extends FlatSpec with Matchers {
  // Get Spark Session
  val spark = SparkSession
    .builder()
    .appName("CSYE 7200 Final Project")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
  val edParentDF = spark.read.parquet(path + "edParentDF")
  val edChildrenDF = spark.read.parquet(path + "edChildrenDF")
  val veDF = spark.read.parquet(path + "veDF")
  val GraphFramesSearch = new GraphFramesSearch

  behavior of "GraphFramesSearch gatPathToRoot"
  it should "work for 9606 to root" in {
    GraphFramesSearch.gatPathToRoot(edParentDF,9606,List(9606)) shouldBe
      List(1, 131567, 2759, 33154, 33208, 6072, 33213, 33511, 7711, 89593, 7742, 7776, 117570, 117571, 8287, 1338369, 32523, 32524, 40674, 32525, 9347, 1437010, 314146, 9443, 376913, 314293, 9526, 314295, 9604, 207598, 9605, 9606)
  }

  behavior of "GraphFramesSearch isSubTree"
  it should "work for vid 1 is a subtree of 9606" in {
    GraphFramesSearch.isSubTree(edParentDF,1,9606) shouldBe false
  }
  it should "work for vid 9606 is a subtree of 1" in {
    GraphFramesSearch.isSubTree(edParentDF,9606,1) shouldBe true
  }

  behavior of "GraphFramesSearch findVidByName"
  it should "work for root, whoes id is 1" in {
    GraphFramesSearch.findVidByName(veDF,"root") shouldBe 1
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    GraphFramesSearch.findVidByName(veDF,"Homo sapiens") shouldBe 9606
  }

  behavior of "GraphFramesSearch getSiblings"
  it should "work for root, whoes id is 1" in {
    GraphFramesSearch.getSiblings(edParentDF,1) shouldBe Nil
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    GraphFramesSearch.getSiblings(edParentDF,9606) shouldBe List(1425170)
  }

  behavior of "GraphFramesSearch getChildren"
  it should "work for root, whoes id is 1" in {
    GraphFramesSearch.getChildren(edParentDF,1) shouldBe List(10239, 12884, 12908, 28384, 131567)
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    GraphFramesSearch.getChildren(edParentDF,9606) shouldBe List(63221, 741158)
  }
  it should "work for \nHomo sapiens neanderthalensis,whose id is 63221" in {
    GraphFramesSearch.getChildren(edParentDF,63221) shouldBe Nil
  }
}
