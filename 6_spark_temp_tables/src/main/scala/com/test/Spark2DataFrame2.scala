package com.test

import org.apache.spark.sql.SparkSession

object Spark2DataFrame2 {
  
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header" -> "true", "inferSchema" -> "true")
    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\FL_insurance_sample.csv")

   // val temptable =  dataFrame.createTempView("cust_ins")
     val temptable =  dataFrame.createOrReplaceTempView("cust_ins")
    
    val rows = sparkSession.sql("select * from cust_ins where construction='Wood'")
    rows.show();
    
  }
}