package com.test

import org.apache.spark.sql.SparkSession

object Spark1DataFrame1 {
  
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header" -> "true", "inferSchema" -> "true")
    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle.csv")

    val temptable =  dataFrame.registerTempTable("cust_veh")
    
    val rows = sparkSession.sql("select * from cust_veh")
    rows.show();
    
  }
}