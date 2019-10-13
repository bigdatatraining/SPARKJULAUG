/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object kafkaconsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumer").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics = "test"
    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "bootstrap.servers"->"localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kaf",
      "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,         // this is to run the
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    print(messages.map(x=>x.value)+"test")

    if(messages!=null) {

      messages.foreachRDD { x =>
val data = x.map(x=>x.value)
  println(s"processing first line : $x")
  val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
  import spark.implicits._
  //val df1=spark.read.format("csv").option("delimiter"," ").load(data)
  //val dfschema=df1.schema()

  val df = spark.read.json(data)
df.printSchema()
  // val df = spark.read.format("csv").schema(dfschema).load(data)
  //df.show()
        df.createOrReplaceTempView("tab")
        val res = spark.sql("select nationality, r.user.cell, r.user.email, seed from tab lateral view explode(results) t as r ")
        res.show()
 // val oUrl = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
 // val oProp = new java.util.Properties()
  //oProp.setProperty("user","ousername")
 // oProp.setProperty("password","opassword")
 // oProp.setProperty("driver","oracle.jdbc.OracleDriver")
  //df.createOrReplaceTempView("personal")
 // df.write.mode("append").jdbc(oUrl,"kafkalogssunil",oProp)
        res.write.format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","weblogs").mode("append").save()

      }
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}
*/
