package com.rdd.spark2

import org.apache.spark.sql.SparkSession

object Spark2RddwithJson {
  def main(args: Array[String]): Unit = {
     val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header"-> "true","inferSchema"-> "true")
    val dataFrame = sparkSession.read.
        
          options(options).
          json("D:\\Practice\\spark-scala\\data\\customer.json")

      dataFrame.printSchema();
      dataFrame.show();
      println("Count:"+dataFrame.count());
  }
}