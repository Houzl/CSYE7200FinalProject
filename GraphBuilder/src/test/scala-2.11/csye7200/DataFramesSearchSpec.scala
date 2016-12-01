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
  val edParentDFW = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
  val veDFW = DataFramesBuilder.getVerticesDF(verticesPath, spark)
  edParentDFW map (_.write.mode(SaveMode.Overwrite).parquet(path + "edParentDF"))
  veDFW map (_.write.mode(SaveMode.Overwrite).parquet(path + "veDF"))
  val edParentDF = spark.read.parquet(path + "edParentDF").persist(StorageLevel.MEMORY_ONLY).cache()
  val veDF = spark.read.parquet(path + "veDF").persist(StorageLevel.MEMORY_ONLY).cache()

  behavior of "DataFramesSearch gatPathToRoot"
  it should "work for 2759 to root" in {
    DataFramesSearch.getPathToRoot(edParentDF,2759,List(2759)) shouldBe
      List(1, 131567, 2759)
  }
  it should "work for -1 to root" in {
    DataFramesSearch.getPathToRoot(edParentDF,-1,List(-1)) shouldBe Nil
  }
  it should "work for 1 to root" in {
    DataFramesSearch.getPathToRoot(edParentDF,1,List(1)) shouldBe Nil
  }

  behavior of "DataFramesSearch isSubTree"
  it should "work for vid 1 is a subtree of 2759" in {
    DataFramesSearch.isSubTree(edParentDF,1,2759) shouldBe false
  }
  it should "work for vid 2759 is a subtree of 1" in {
    DataFramesSearch.isSubTree(edParentDF,2759,1) shouldBe true
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

  behavior of "DataFramesSearch getSiblings"
  it should "work for root, whoes id is 1" in {
    DataFramesSearch.getSiblings(edParentDF,1) shouldBe Nil
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    DataFramesSearch.getSiblings(edParentDF,9606) shouldBe List(1425170)
  }
  it should "work for None,whose id is -1" in {
    DataFramesSearch.getSiblings(edParentDF,-1) shouldBe Nil
  }

  behavior of "DataFramesSearch getChildren"
  it should "work for root, whoes id is 1" in {
    DataFramesSearch.getChildren(edParentDF,1) shouldBe List(10239, 12884, 12908, 28384, 131567)
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    DataFramesSearch.getChildren(edParentDF,9606) shouldBe List(63221, 741158)
  }
  it should "work for Homo sapiens neanderthalensis,whose id is 63221" in {
    DataFramesSearch.getChildren(edParentDF,63221) shouldBe Nil
  }
  it should "work for None,whose id is -1" in {
    DataFramesSearch.getChildren(edParentDF,-1) shouldBe Nil
  }
}
