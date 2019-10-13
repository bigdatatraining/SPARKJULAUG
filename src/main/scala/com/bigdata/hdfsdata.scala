package com.bigdata

import org.apache.spark.sql.SparkSession

object hdfsdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("hdfsdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    spark.stop()
  }
}
