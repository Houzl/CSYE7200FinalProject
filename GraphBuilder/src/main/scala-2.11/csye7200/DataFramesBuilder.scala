package csye7200

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by houzl on 11/18/2016.
  */
object DataFramesBuilder{

  /**
    * Build parent edges DataFrame from file
    * @param edgesPath edges file path "nodes.dmp"
    * @param spark SparkSession
    * @return Try[DataFrame] of edges
    */
  def getEdgesParentDF(edgesPath : String, spark : SparkSession): Try[DataFrame] ={
    val edgesTuple = Try(spark.sparkContext.textFile(edgesPath)
      .map(_.split('|'))
      .map(line => (line.head.trim.toLong, line.tail.head.trim.toLong, "parent")))
    edgesTuple map(spark.createDataFrame(_).filter("_1 != _2").toDF("src", "dst", "relationship"))
  }

  /**
    * Build vertices DataFrame from file
    * @param verticesPath vertices file path "names.dmp"
    * @param spark SparkSession
    * @return Try[DataFrame] of vertices
    */
  def getVerticesDF(verticesPath: String, spark : SparkSession): Try[DataFrame] ={
    val verticesTuple = Try(spark.sparkContext.textFile(verticesPath)
      .map(_.split('|'))
      .map(line => (line.head.trim.toLong, line.tail.head.trim))
      .groupByKey()
      .map(line => (line._1, line._2.foldLeft(","){(b,a) => b + a + ","})))
    verticesTuple map(spark.createDataFrame(_).toDF("id", "name"))
  }

  /**
    * Build path to root DataFrame. But too slow to run.
    * @param edParentDF parent edges dataframe
    * @param spark SparkSession
    * @param maxLevel Max level to travel. -1 means travel all nodes.
    * @return DataFrame, id, pathToRoot String.
    */
  def buildPathToRootDF(edParentDF: DataFrame, spark : SparkSession, maxLevel: Int): DataFrame = {
    /**
      * tailrec to build pathToRoot Map
      * @param dst List of ids
      * @param level current level
      * @param maxLevel Max level to travel. -1 means travel all nodes.
      * @return List of ids for dst's Children.
      */
    @tailrec def bfsTraversal(dst: List[Long], level: Int, maxLevel: Int, p : Map[Long, String]): Map[Long, String] = {
      //nextLevelTuple List of (id, List(ids for children))
      val nextLevelTuple = dst.map(id => (id, edParentDF.filter(s"dst = $id").select("src").collect().toList.map(_.getLong(0))))
      val newList = for (t <- nextLevelTuple) yield{
        val parentId = t._1
        val parentPath = p.getOrElse(parentId,"/")
        for (id <- t._2) yield (id -> s"$parentPath$parentId/")
      }
      val newMap = newList.flatMap(_.map(i => i)).toMap[Long, String] ++ p
      val nextLevelIds = nextLevelTuple flatMap (i => i._2.map(j => j))
      if (nextLevelIds.isEmpty || (maxLevel != -1 && level >= maxLevel)) newMap
      else bfsTraversal(nextLevelIds,level + 1,maxLevel, newMap)
    }
    spark.createDataFrame(bfsTraversal(List(1L),1,maxLevel, Map(1L -> "/")).toSeq).toDF("id","path")
  }

  def buildPathToRootDF2(edParentDF: DataFrame, spark : SparkSession, maxLevel: Int): DataFrame = {
    /**
      * tailrec to build pathToRoot Map
      * @param dst List of ids
      * @param level current level
      * @param maxLevel Max level to travel. -1 means travel all nodes.
      * @return List of ids for dst's Children.
      */
    @tailrec def bfsTraversal(dst: RDD[Long], level: Int, maxLevel: Int, p : Map[Long, String]): Map[Long, String] = {
      //nextLevelTuple List of (id, List(ids for children))
      val nextLevelTuple = dst.map(id => (id, edParentDF.filter(s"dst = $id").select("src").rdd.map(_.getLong(0))))
      val newRDD = for (t <- nextLevelTuple) yield{
        val parentId = t._1
        val parentPath = p.getOrElse(parentId,"/")
        for (id <- t._2) yield (id -> s"$parentPath$parentId/")
      }
      val newMap = newRDD.flatMap(_.collect()).collectAsMap().toMap ++ p
      val nextLevelIds = newRDD.flatMap(_.collect().map(_._1))
      if (nextLevelIds.isEmpty || (maxLevel != -1 && level >= maxLevel)) newMap
      else bfsTraversal(nextLevelIds,level + 1,maxLevel, newMap)
    }
    val map = bfsTraversal(spark.sparkContext.parallelize(List(1L)),1,maxLevel, Map(1L -> "/"))
    spark.createDataFrame(map.toSeq).toDF("id","path")
  }
}
object test extends App{
    val spark = SparkSession
      .builder()
      .appName("CSYE 7200 Final Project")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
    val edgesPath = path + "nodes.dmp"
    val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
    val edDF = edParentDF.getOrElse(spark.createDataFrame(Nil))
    val dst = spark.sparkContext.parallelize(Seq((10239L,"/1/"),(131567L,"/1/")))
    val dst2 = List((10239L,"/1/"),(131567L,"/1/"))
    for (i <- dst) println(i)
    val id = 1L

    val nextLevelDF = dst2.map(i => {
      import org.apache.spark.sql.functions.lit
      val id = i._1
      val parentPath = i._2
      edDF.filter(s"dst = $id").select("src").withColumn("parent", lit(s"$parentPath$id/"))
    })

    for (i <- nextLevelDF) i.show()



//  val nextLevelTuple = dst.map(i => {
//    import org.apache.spark.sql.functions.lit
//    val id = i._1
//    val parentPath = i._2
//    val rankUDF = udf((p:String) => p)
//    (s"$parentPath$id/" , edDF.filter(s"dst = $id").select("src").withColumn("parent", lit(s"$parentPath$id/")).rdd.map(_.getLong(0)))
//  })
//  for (i <- nextLevelTuple) println(i)
//  val newRDD = for (t <- nextLevelTuple) yield{
//    for (id <- t._2) yield (id -> t._1)
//  }
//  for (i <- newRDD)
//    for (j <- i)
//    println(j)

//  val p = Map(12884L -> "/1/", 1L -> "/", 10239L -> "/1/", 131567L -> "/1/", 28384L -> "/1/", 12908L -> "/1/")
//  val newRDD = for (t <- nextLevelTuple) yield{
//    for (id <- t._2) yield (id -> t._1)
//  }
//  val newMap = newRDD.flatMap(_.collect()).collectAsMap().toMap ++ p
//  //val nextLevelIds = spark.sparkContext.parallelize(newMap.keys.toSeq)
//  val nextLevelIds = newRDD.flatMap(_.collect().map(_._1))
//  println(newMap)
//  for (i <-nextLevelIds) println(i)
}
