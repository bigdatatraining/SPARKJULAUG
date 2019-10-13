/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
object getTwitterData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getTwitterData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
       val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val APIkey = ""//APIKey
    val APIsecretkey = ""// (API secret key)
    val Accesstoken = "" //Access token
    val Accesstokensecret = "" //Access token secret

        val searchFilter = "tensorflow,Artificial Intelligence"
    //  val pipelineFile = ""
    //val searchFilter = "BJP, Jammu and Kashmir, Jammu, 370,ladakh"

    val interval = 10
    //  import spark.sqlContext.implicits._
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)

    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))
    // now tweetStream is dstream

    tweetStream.foreachRDD { a =>
      val rdd = a.toString()
import java.util.Date
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      val df1 = a.map(x =>(x.getRetweetCount(), x.getText,x.getUser().getScreenName())).toDF("numOfRetwits","twit","user")
      df1.printSchema()
      df1.show(8,false)
      df1.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where numOfRetwits>500")
    /*  val positive=spark.sql("select * from tab where twit like '%thanks%'")
      val url = """jdbc:oracle:thin://@oradb.cswlcwej6qx5.ap-south-1.rds.amazonaws.com:1521/ORCL"""
      val prop = new java.util.Properties()
      prop.setProperty("user","musername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.OracleDriver")
      val df = spark.read.jdbc(url,"EMP",prop)*/

      //positive.write.mode(saveMode = "append").jdbc(url,"posfeedback",prop)

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
*/
