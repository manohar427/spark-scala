package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType

object RddBasicOperations1 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header" -> "true", "inferSchema" -> "true")
    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle.csv")

    val customerSchema = dataFrame.schema;
    println(customerSchema);
    val customerColumns = dataFrame.columns

    println(customerColumns.mkString(" "))
    
    //YearOfMa
    val yearOfMaDesc = dataFrame.describe("YearOfMa")
    yearOfMaDesc.show();
    
    val colTypes = dataFrame.dtypes;
    println(colTypes.mkString(","));
    
   // dataFrame.head(3).foreach(println)
     dataFrame.show(3)
    
  }
}

