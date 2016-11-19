package csye7200

import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

/**
  * Created by houzl on 11/15/2016.
  */
object GraphFramesBuilderObject extends App{
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.graphx.{Edge, _}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SparkSession
  import org.graphframes.GraphFrame

  val conf = new SparkConf()
  conf.setAppName("Builder")
  conf.setMaster("local[2]")
  val sc = new SparkContext(conf)
  val path = "C:\\Users\\houzl\\Downloads\\taxdmp\\"
  val edgeFile = sc.textFile(path + "nodes.dmp")
  val vertexFile = sc.textFile(path + "names.dmp")

  val sqlContext = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

//  val edgesParentTuple = edgeFile.map(line => {
//    val fields = line.split('|')
//    Edge(fields(0).trim.toLong, fields(1).trim.toLong, "parent")}
//  )

  val edgesParentTuple = edgeFile.map(_.split('|')).map(line => (line.head.trim.toLong, line.tail.head.trim.toLong, "parent"))
  val edgesParentDF = sqlContext.createDataFrame(edgesParentTuple).toDF("src", "dst", "relationship")
  edgesParentDF.persist(StorageLevel.MEMORY_ONLY)
  edgesParentDF.show(1)
  @tailrec def gatPathToRoot(edges: DataFrame, vid: Long, r : List[Long]): List[Long] = {
    val nextSrc = edges.filter("src = " + vid).select("dst").head().getLong(0)
    if (nextSrc == 1) List(nextSrc) ::: r
    else gatPathToRoot(edges, nextSrc, List(nextSrc) ::: r)
  }

//  val vertexTuple = vertexFile.map(line => {
//    val fields = line.split('|')
//    (fields(0).trim.toLong, fields(1).trim)
//  }).groupByKey().map(i => (i._1, i._2.foldLeft(","){(b,a) => b + a + ","}, gatPathToRoot(edgesParentDF, i._1, List(i._1))))
//
//  val vertexDF = sqlContext.createDataFrame(vertexTuple).toDF("id", "name", "path")
//
//  val g = GraphFrame(vertexDF, edgesParentDF)

 // g.vertices.filter("id = 9606").show()

//  val g = GraphFrame(vertexDF, edgesParentDF)
//  g.persist(StorageLevel.MEMORY_ONLY)

//  val edgesTupleChildren: RDD[Edge[String]] = edgeFile.map(line => {
//    val fields = line.split('|')
//    Edge(fields(1).trim.toLong, fields(0).trim.toLong, "children")}
//  )
//
//  val edgesTuple = edgesTupleParent union edgesTupleChildren
//
//  val edgesDF = sqlContext.createDataFrame(edgesTuple).toDF("src", "dst", "relationship")
//
//  val g = GraphFrame(vertexDF, edgesDF)
//
//  g.vertices.write.parquet("C:\\Users\\houzl\\Downloads\\taxdmp\\vertices5")
//  g.edges.write.parquet("C:\\Users\\houzl\\Downloads\\taxdmp\\edges5")
}

