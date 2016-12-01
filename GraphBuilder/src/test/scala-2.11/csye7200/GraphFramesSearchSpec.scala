package csye7200

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
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
  val edgesPath = path + "nodes.dmp"
  val verticesPath = path + "names.dmp"
  val edParentDFW = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
  val veDFW = DataFramesBuilder.getVerticesDF(verticesPath, spark)
  edParentDFW map (_.write.mode(SaveMode.Overwrite).parquet(path + "edParentDF"))
  veDFW map (_.write.mode(SaveMode.Overwrite).parquet(path + "veDF"))

  val edParentDF = spark.read.parquet(path + "edParentDF").cache()
  val veDF = spark.read.parquet(path + "veDF").persist(StorageLevel.MEMORY_ONLY).cache()

  val graph = GraphFrame(veDF, edParentDF).cache()

  behavior of "GraphFramesSearch getPathToRoot"
  it should "work for 9606 to root" in {
    GraphFramesSearch.getPathToRoot(graph,2759,List(2759)) shouldBe
      List(1, 131567, 2759)
  }
  it should "work for -1 to root" in {
    GraphFramesSearch.getPathToRoot(graph,-1,List(-1)) shouldBe Nil
  }
  it should "work for 1 to root" in {
    GraphFramesSearch.getPathToRoot(graph,1,List(1)) shouldBe Nil
  }

  behavior of "GraphFramesSearch isSubTree"
  it should "work for vid 1 is a subtree of 2759" in {
    GraphFramesSearch.isSubTree(graph,1,2759) shouldBe false
  }
  it should "work for vid 2759 is a subtree of 1" in {
    GraphFramesSearch.isSubTree(graph,2759,1) shouldBe true
  }

  behavior of "GraphFramesSearch getSiblings"
  it should "work for root, whose id is 1" in {
    GraphFramesSearch.getSiblings(graph,1) shouldBe Nil
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    GraphFramesSearch.getSiblings(graph,9606) shouldBe List(1425170)
  }
  it should "work for None,whose id is -1" in {
    GraphFramesSearch.getSiblings(graph,-1) shouldBe Nil
  }

  behavior of "GraphFramesSearch getChildren"
  it should "work for root, whose id is 1" in {
    GraphFramesSearch.getChildren(graph,1) shouldBe List(10239, 12884, 12908, 28384, 131567)
  }
  it should "work for Homo sapiens,whose id is 9606" in {
    GraphFramesSearch.getChildren(graph,9606) shouldBe List(63221, 741158)
  }
  it should "work for Homo sapiens neanderthalensis,whose id is 63221" in {
    GraphFramesSearch.getChildren(graph,63221) shouldBe Nil
  }
  it should "work for None,whose id is -1" in {
    GraphFramesSearch.getChildren(graph,-1) shouldBe Nil
  }
}
