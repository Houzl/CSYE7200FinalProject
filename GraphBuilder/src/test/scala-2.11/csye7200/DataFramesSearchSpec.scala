package csye7200

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by houzl on 11/18/2016.
  */
class DataFramesSearchSpec extends FlatSpec with Matchers {
  // Get Spark Session
  val spark = SparkSession
    .builder()
    .appName("CSYE 7200 Final Project")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
  val edgesPath = path + "nodes.dmp"
  val verticesPath = path + "names.dmp"
  val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark).getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
  val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark).getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()

  behavior of "DataFramesSearch getPathToRoot"
  it should "work for 12429 to root" in {
    DataFramesSearch.getPathToRoot(edParentDF,12429,List(12429)) shouldBe List(1, 10239, 12429)
  }
  it should "work for -1 to root" in {
    DataFramesSearch.getPathToRoot(edParentDF,-1,List(-1)) shouldBe Nil
  }
  it should "work for 1 to root" in {
    DataFramesSearch.getPathToRoot(edParentDF,1,List(1)) shouldBe Nil
  }

  behavior of "DataFramesSearch isSubTree"
  it should "work for vid 1 is a subtree of 12429" in {
    DataFramesSearch.isSubTree(edParentDF,1,12429) shouldBe false
  }
  it should "work for vid 12429 is a subtree of 1" in {
    DataFramesSearch.isSubTree(edParentDF,12429,1) shouldBe true
  }

  behavior of "DataFramesSearch findVidByName"
  it should "work for root, whose id is 1" in {
    DataFramesSearch.findVidByName(veDF,"root") shouldBe 1
  }
  it should "work for Homo sapiens, whose id is 9606" in {
    DataFramesSearch.findVidByName(veDF,"Homo sapiens") shouldBe 9606
  }
  it should "work for ....,who does not exist" in {
    DataFramesSearch.findVidByName(veDF,"...") shouldBe -1
  }

  behavior of "DataFramesSearch findNameByVID"
  it should "work for root, whose id is 1" in {
    DataFramesSearch.findNameByVID(veDF,1) shouldBe ",all,root,"
  }
  it should "work for Homo sapiens, whose id is 9606" in {
    DataFramesSearch.findNameByVID(veDF, 9606) shouldBe ",Homo sapiens,Homo sapiens Linnaeus, 1758,human,humans,man,"
  }
  it should "work for ....,who does not exist" in {
    DataFramesSearch.findNameByVID(veDF, -1) shouldBe ""
  }

  behavior of "DataFramesSearch getSiblings"
  it should "work for root, whoes id is 1" in {
    DataFramesSearch.getSiblings(edParentDF,1) shouldBe Nil
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    DataFramesSearch.getSiblings(edParentDF,10239).toSet shouldBe Set(12884, 12908, 28384, 131567)
  }
  it should "work for None,whose id is -1" in {
    DataFramesSearch.getSiblings(edParentDF,-1) shouldBe Nil
  }

  behavior of "DataFramesSearch getChildren"
  it should "work for root, whoes id is 1" in {
    DataFramesSearch.getChildren(edParentDF,1).toSet shouldBe Set(10239, 12884, 12908, 28384, 131567)
  }
  it should "work for 10239" in {
    DataFramesSearch.getChildren(edParentDF,10239).toSet shouldBe
      Set(35325, 439488, 552364, 39759, 451344, 12333, 1425366, 12429, 35268, 12877, 35237, 186616, 29258, 1714266, 686617)
  }
  it should "work for Homo sapiens neanderthalensis,whose id is 63221" in {
    DataFramesSearch.getChildren(edParentDF,63221) shouldBe Nil
  }
  it should "work for None,whose id is -1" in {
    DataFramesSearch.getChildren(edParentDF,-1) shouldBe Nil
  }
}
