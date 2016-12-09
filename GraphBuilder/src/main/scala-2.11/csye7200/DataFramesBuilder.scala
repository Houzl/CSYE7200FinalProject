package csye7200

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
    * Build path to root DataFrame. Using List in recursion
    * @param edParentDF parent edges dataframe
    * @param spark SparkSession
    * @param maxLevel Max level to travel. -1 means travel all nodes.
    * @return DataFrame, id, pathToRoot String.
    */
  def buildPathToRootDF(edParentDF: DataFrame, spark : SparkSession, maxLevel: Int): DataFrame = {
    /**
      * tailrec to build pathToRoot Map, Using DataFrame in recursion
      * @param curLevelDF currentLevel nodes (id,path) in DataFrame
      * @param level current level
      * @param maxLevel Max level to travel. -1 means travel all nodes.
      * @param previousDF all parsed nodes (id,path) in DataFrame
      * @return List of ids for dst's Children.
      */
    @tailrec def bfsTraversalUsingDF(curLevelDF: DataFrame, level: Int, maxLevel: Int, previousDF : DataFrame): DataFrame = {
      val nextLevelDF = curLevelDF.collect().toList.map(r => (r.getLong(0),r.getString(1))).map(i => {
        import org.apache.spark.sql.functions.lit
        val id = i._1
        val parentPath = i._2
        edParentDF.filter(s"dst = $id").select("src").withColumn("path", lit(s"$parentPath$id/"))
      }).reduceLeft(_.union(_))
      val wholeDF = previousDF.union(nextLevelDF)
      if (nextLevelDF.count() == 0 || (maxLevel != -1 && level >= maxLevel)) wholeDF
      else bfsTraversalUsingDF(nextLevelDF,level + 1,maxLevel, wholeDF)
    }
    val rootDF = spark.createDataFrame(Seq((1L,"/"))).toDF("id","path")
    bfsTraversalUsingDF(rootDF,1,3,rootDF)
  }
}
//object test extends App{
//  val spark = SparkSession
//    .builder()
//    .appName("CSYE 7200 Final Project")
//    .master("local[2]")
//    .config("spark.some.config.option", "some-value")
//    .getOrCreate()
//  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
//  val edgesPath = path + "nodes.dmp"
//  val edParentDF = DataFramesBuilder.getEdgesParentDF(edgesPath, spark)
//  val edDF = edParentDF.getOrElse(spark.createDataFrame(Nil))
//  val dst = spark.sparkContext.parallelize(Seq((10239L,"/1/"),(131567L,"/1/")))
//  val dst2 = List((10239L,"/1/"),(131567L,"/1/"))
//  for (i <- dst) println(i)
//  val id = 1L
//
//  val curLevelDF = dst2.map(i => {
//    import org.apache.spark.sql.functions.lit
//    val id = i._1
//    val parentPath = i._2
//    edDF.filter(s"dst = $id").select("src").withColumn("parent", lit(s"$parentPath$id/"))
//  }).reduceLeft(_.union(_))
//
//
//    //for (i <- nextLevelDF) i.show()
//    nextLevelDF.show()
//    val nextLevelDF = curLevelDF.collect().toList.map(r => (r.getLong(0),r.getString(1))).map(i => {
//      import org.apache.spark.sql.functions.lit
//      val id = i._1
//      val parentPath = i._2
//      edDF.filter(s"dst = $id").select("src").withColumn("parent", lit(s"$parentPath$id/"))
//    }).reduceLeft(_.union(_))
//
//
//
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
//
//  val p = Map(12884L -> "/1/", 1L -> "/", 10239L -> "/1/", 131567L -> "/1/", 28384L -> "/1/", 12908L -> "/1/")
//  val newRDD = for (t <- nextLevelTuple) yield{
//    for (id <- t._2) yield (id -> t._1)
//  }
//  val newMap = newRDD.flatMap(_.collect()).collectAsMap().toMap ++ p
//  //val nextLevelIds = spark.sparkContext.parallelize(newMap.keys.toSeq)
//  val nextLevelIds = newRDD.flatMap(_.collect().map(_._1))
//  println(newMap)
//  for (i <-nextLevelIds) println(i)
//}
