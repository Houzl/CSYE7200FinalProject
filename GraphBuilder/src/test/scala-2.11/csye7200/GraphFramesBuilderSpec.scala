package csye7200

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by houzl on 11/18/2016.
  */
class GraphFramesBuilderSpec extends FlatSpec with Matchers {
  // Get Spark Session
  val spark = SparkSession
    .builder()
    .appName("CSYE 7200 Final Project")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"

  behavior of "GraphFramesBuilder"
  it should "work for read from original file" in {
    val edgesPath = path + "nodes.dmp"
    val verticesPath = path + "names.dmp"
    val GraphFramesBuilder = new GraphFramesBuilder(verticesPath, edgesPath, spark);
    val edParentDF = GraphFramesBuilder.getEdgesParentDF
    val edChildrenDF = GraphFramesBuilder.getEdgesChildrenDF
    val veDF = GraphFramesBuilder.getVerticesDF
    val edSchema = List(StructField("src",LongType,false), StructField("dst",LongType,false), StructField("relationship",StringType,true))
    edParentDF.count() shouldBe 1528460
    edParentDF.schema.fields.toList shouldBe edSchema
    edChildrenDF.count() shouldBe 1528460
    edChildrenDF.schema.fields.toList shouldBe edSchema
    veDF.count() shouldBe 1528461
    veDF.schema.fields.toList shouldBe List(StructField("id",LongType,false), StructField("name",StringType,true))

    val edParentDFLoad = spark.read.parquet(path + "edParentDF")
    val edChildrenDFLoad = spark.read.parquet(path + "edChildrenDF")
    val veDFLoad = spark.read.parquet(path + "veDF")
    edParentDF.count() shouldBe 1528460
    edParentDF.schema.fields.toList shouldBe edSchema
    edChildrenDF.count() shouldBe 1528460
    edChildrenDF.schema.fields.toList shouldBe edSchema
    veDF.count() shouldBe 1528461
    veDF.schema.fields.toList shouldBe List(StructField("id",LongType,false), StructField("name",StringType,true))
  }

  it should "work for read from parquet file" in {
    val edParentDF = spark.read.parquet(path + "edParentDF")
    val edChildrenDF = spark.read.parquet(path + "edChildrenDF")
    val veDF = spark.read.parquet(path + "veDF")
    val edSchema = List(StructField("src",LongType,true), StructField("dst",LongType,true), StructField("relationship",StringType,true))
    edParentDF.count() shouldBe 1528460
    edParentDF.schema.fields.toList shouldBe edSchema
    edChildrenDF.count() shouldBe 1528460
    edChildrenDF.schema.fields.toList shouldBe edSchema
    veDF.count() shouldBe 1528461
    veDF.schema.fields.toList shouldBe List(StructField("id",LongType,true), StructField("name",StringType,true))
  }
}
