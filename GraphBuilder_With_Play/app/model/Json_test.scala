package model

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by mali on 12/5/16.
  */

object Json_test{
//   val spark = SparkSession
//     .builder()
//     .appName("CSYE 7200 Final Project2")
//     .master("local[2]")
//     .config("spark.some.config.option", "some-value")
//     .getOrCreate()
//   val path = "/Users/mali/Downloads/taxdmp/"

//   val edgesPath = path + "nodes.dmp"
//   val verticesPath = path + "names.dmp"
//   val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
//   val veDF = DataFramesBuilder.getVerticesDF(verticesPath, spark)
//   val df = edParentDF.getOrElse(spark.createDataFrame(List())).persist(StorageLevel.MEMORY_ONLY).cache()
//   val bv = DataFramesBuilder.buildPathToRootDF(df, spark, 3)

}