package csye7200

import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success, Try}

/**
  * Created by houzl on 11/18/2016.
  */
class DataFramesBuilderSpec extends FlatSpec with Matchers {
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
  val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
  val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
  edParentDF map (_.write.mode(SaveMode.Overwrite).parquet(path + "edParentDF"))
  veDF map (_.write.mode(SaveMode.Overwrite).parquet(path + "veDF"))

  behavior of "GraphFramesBuilder"
  it should "work for read from wrong original file" in {
    val edgesPath = path + "nodes"
    val verticesPath = path + "names"
    val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
    val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)

    edParentDF match {
      case f @ _ => f.isFailure shouldBe true
    }

    veDF match {
      case f @ _ => f.isFailure shouldBe true
    }
  }

  it should "work for read from original file" in {
    val edgesPath = path + "nodes.dmp"
    val verticesPath = path + "names.dmp"
    val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
    val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
    val edSchema = List(StructField("src",LongType,false), StructField("dst",LongType,false), StructField("relationship",StringType,true))

    edParentDF match {
      case Success(n) => {
        n.count() shouldBe 1528460
        n.schema.fields.toList shouldBe edSchema
      }
      case Failure(x) => x shouldBe Nil
    }

    veDF match {
      case Success(n) => {
        n.count() shouldBe 1528461
        n.schema.fields.toList shouldBe List(StructField("id",LongType,false), StructField("name",StringType,true))
      }
      case Failure(x) => x shouldBe Nil
    }
  }

  it should "work for read from parquet file" in {
    val edParentDF = Try(spark.read.parquet(path + "edParentDF"))
    val veDF = Try(spark.read.parquet(path + "veDF"))
    val edSchema = List(StructField("src",LongType,true), StructField("dst",LongType,true), StructField("relationship",StringType,true))

    edParentDF match {
      case Success(n) => {
        n.count() shouldBe 1528460
        n.schema.fields.toList shouldBe edSchema
      }
      case Failure(x) => x
    }

    veDF match {
      case Success(n) => {
        n.count() shouldBe 1528461
        n.schema.fields.toList shouldBe List(StructField("id",LongType,true), StructField("name",StringType,true))
      }
      case Failure(x) => x shouldBe Nil
    }
  }

  it should "work for buildPathToRoot from edParentDF" in {
    val df = edParentDF.getOrElse(spark.read.parquet(path + "edParentDF")).persist(StorageLevel.MEMORY_ONLY).cache()
    val bv = DataFramesBuilder.buildPathToRootDF(df, spark, 3)
    bv.write.mode(SaveMode.Overwrite).parquet(path + "pathToRootDF")
    bv.count() shouldBe 2027
    bv.filter("id = 1").select("path").head().getString(0) shouldBe "/"
  }
}
