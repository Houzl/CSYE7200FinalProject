package csye7200

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by houzl on 12/1/2016.
  */
class IndexDataFramesSearchSpec extends FlatSpec with Matchers {
  // Get Spark Session
  val spark = SparkSession
    .builder()
    .appName("CSYE 7200 Final Project")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
  val edgesPath = path + "nodes.dmp"
  val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark).getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
  val pathToRootDFOri = DataFramesBuilder.buildPathToRootDF(edParentDF, spark, 3)
  //pathToRootDFOri.write.mode(SaveMode.Overwrite).parquet(s"$path\\pathToRootDF")
  val pathToRootDF = spark.read.parquet(s"$path\\pathToRootDF").persist(StorageLevel.MEMORY_ONLY).cache()


  behavior of "IndexDataFramesSearch getPathToRoot"
  it should "work for 12429 to root" in {
    IndexDataFramesSearch.getPathToRoot(pathToRootDF,12429,List(12429)) shouldBe List(1, 10239, 12429)
  }
  it should "work for -1 to root" in {
    IndexDataFramesSearch.getPathToRoot(pathToRootDF,-1,List(-1)) shouldBe Nil
  }
  it should "work for 1 to root" in {
    IndexDataFramesSearch.getPathToRoot(pathToRootDF,1,List(1)) shouldBe Nil
  }

  behavior of "IndexDataFramesSearch isSubTree"
  it should "work for vid 1 is a subtree of 12429" in {
    IndexDataFramesSearch.isSubTree(pathToRootDF,1,12429) shouldBe false
  }
  it should "work for vid 12429 is a subtree of 1" in {
    IndexDataFramesSearch.isSubTree(pathToRootDF,12429,1) shouldBe true
  }

  behavior of "IndexDataFramesSearch getSiblings"
  it should "work for root, whoes id is 1" in {
    IndexDataFramesSearch.getSiblings(pathToRootDF,1) shouldBe Nil
  }
  it should "work for id 10239" in {
    IndexDataFramesSearch.getSiblings(pathToRootDF,10239).toSet shouldBe Set(12884, 12908, 28384, 131567)
  }
  it should "work for None,whose id is -1" in {
    IndexDataFramesSearch.getSiblings(pathToRootDF,-1) shouldBe Nil
  }

  behavior of "IndexDataFramesSearch getChildren"
  it should "work for root, whose id is 1" in {
    IndexDataFramesSearch.getChildren(pathToRootDF,1).toSet shouldBe Set(10239, 12884, 12908, 28384, 131567)
  }
  it should "work for 10239" in {
    IndexDataFramesSearch.getChildren(pathToRootDF,10239).toSet shouldBe
      Set(35325, 439488, 552364, 39759, 451344, 12333, 1425366, 12429, 35268, 12877, 35237, 186616, 29258, 1714266, 686617)
  }
  it should "work for Homo sapiens neanderthalensis,whose id is 63221" in {
    IndexDataFramesSearch.getChildren(pathToRootDF,63221) shouldBe Nil
  }
  it should "work for None,whose id is -1" in {
    IndexDataFramesSearch.getChildren(pathToRootDF,-1) shouldBe Nil
  }
}
