package com.test

import org.apache.spark.sql.SparkSession

object RddBasicOperations2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header" -> "true", "inferSchema" -> "true")
    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle.csv")
    //dataFrame.show(2)
    val filteredDF = dataFrame.select("YearOfMa","Country")
    //filter(dataFrame("Country")==="India")
    //where("Country=='India'")
    
    
    .groupBy("Country").count()
    filteredDF.show()
  }
}