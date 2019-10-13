package com.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//com.bigdata.getgoodBankbalanceHolders

object getgoodBankbalanceHolders {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getgoodBankbalanceHolders").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
//val data = "C:\\work\\datasets\\bank-full.csv"
   val data = args(0)
   val output = args(1)
    val df = spark.read.format("csv").option("delimiter",";").option("header","true").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab where balance>50000 order by balance desc")
    res.show()
    res.coalesce(1).write.format("csv").option("header","true").save(output)

    val che = res.where()
    spark.stop()
  }
}
