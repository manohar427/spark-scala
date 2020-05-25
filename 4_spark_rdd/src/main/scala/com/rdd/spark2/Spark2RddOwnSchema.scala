package com.rdd.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType

object Spark2RddOwnSchema {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val options = Map("header"-> "true")
    val ownSchema = StructType(
        StructField("YearOfMa",LongType,true) ::
        StructField("Vehicle Type",StringType,true) ::
        StructField("Country",StringType,true) ::
        StructField("No Of Vehicles",LongType,true) ::Nil
    )
    val dataFrame = sparkSession.read.
        
          options(options).
          schema(ownSchema).
          csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle.csv")

      dataFrame.printSchema();
      dataFrame.show();
      println("Count:"+dataFrame.count());
      
      /*root
 |-- YearOfMa: integer (nullable = true)
 |-- Vehicle Type: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- No Of Vehicles: integer (nullable = true)
*/
      
      /*
       root
 |-- YearOfMa: long (nullable = true)
 |-- Vehicle Type: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- No Of Vehicles: long (nullable = true)
       */
  }
}

