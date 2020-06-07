package com.test

import org.apache.spark.sql.SparkSession


case class Vehicle(YearOfMa:Integer,VehicleType:String,Country:String,NoOfVehicles:Integer)

object SparkDataSet1 {
  def main(args: Array[String]): Unit = {
    
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header" -> "true", "inferSchema" -> "true")
    import sparkSession.implicits._
    
    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle2.csv")
    .as[Vehicle]
    
    dataFrame.show()
  }
}