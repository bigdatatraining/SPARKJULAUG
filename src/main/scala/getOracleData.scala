/*


import java.util.Date

//import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.slf4j.LoggerFactory
//import org.slf4j.impl.Log4jLoggerFactory


object getOracleData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOracleData").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    //val logger = Logger(LoggerFactory.getLogger(getClass.getName))
val tab = "EMP"
    val startTime = new Date

    import spark.implicits._
    import spark.sql
    //logger.warn("connecting oracle database to get info from $tab table")
    val url = """jdbc:oracle:thin://@oradb.cswlcwej6qx5.ap-south-1.rds.amazonaws.com:1521/ORCL"""
    val prop = new java.util.Properties()
    prop.setProperty("user","musername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    prop.setProperty("fetchsize", "100000") // req huge amount of resources.
    /*prop.setProperty("partitionColumn", "FLIGHTNUM")
    prop.setProperty("lowerBound", "1")
    prop.setProperty("upperBound", "7009728")
    prop.setProperty("numPartitions", "753")
    */
//    val stride = upperBound / numPartitions – lowerBound / numPartitions
//    SELECT * FROM table WHERE FLIGHTNUM BETWEEN 1 AND 10000
    //  SELECT * FROM table WHERE FLIGHTNUM BETWEEN 10000 AND 20000






    //val endTime = new Date
    val endTime = new Date()
    val totaltime = endTime.getTime - startTime.getTime
    val minutes = ((totaltime / (1000 * 60)) % 60);
  //  logger.warn("to process %s start %s and end time %s total time: %s takes in minutes %s".format(tab, startTime, endTime, totaltime, minutes))



    val df = spark.read.jdbc(url,tab,prop)
    //df.show()
    df.createOrReplaceTempView("tab")
    val  abc = spark.sql("select * from tab where sal>2999")
//abc.show()
    //abc.write.jdbc(url,"sal3000plus",prop)
    abc.write.mode(saveMode = "overwrite").saveAsTable("newtab")
//logger.info("successfully executed")
    spark.stop()
  }
}
*/
