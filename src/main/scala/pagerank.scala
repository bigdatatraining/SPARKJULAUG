/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
package org.apache.spark.examples.graphx

// $example on$
import org.apache.spark.graphx.GraphLoader
object pagerank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("pagerank").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
val follower = args(0) //"C:\\work\\spark-2.4.3-bin-hadoop2.7\\data\\graphx\\followers.txt"
    val userdata = args(1) //"C:\\work\\spark-2.4.3-bin-hadoop2.7\\data\\graphx\\users.txt"

        import spark.implicits._
    import spark.sql
    val graph = GraphLoader.edgeListFile(sc, follower)
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users1 = sc.textFile(userdata)
      val users = users1.map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    spark.stop()
  }
}
*/
