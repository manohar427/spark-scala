package com.test

import org.apache.spark.sql.SparkSession

case class Vehicle2(YearOfMa: Integer, VehicleType: String, Country: String, NoOfVehicles: Integer)
case class Vehicle3(VehicleType:String,Country:String); 

object SparkDataSet2 {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header" -> "true", "inferSchema" -> "true")
    import sparkSession.implicits._

    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle2.csv")
      .as[Vehicle2]

    dataFrame.show()

   //  val filteredRDD = dataFrame.filter(row => row.Country == "India")

   //    val filteredRDD = dataFrame.where(dataFrame("Country") === "India")
   // val filteredRDD = dataFrame.where("Country == 'India'")
    
   //     filteredRDD.show()
   //  println("COUNT:"+filteredRDD.count());

   //Select query will results as DataFrame not as DataSet
    
   // import sparkSession.implicits._
    val df1 =  dataFrame.select("VehicleType", "Country").as[Vehicle3]
    df1.show()
   
  }
}