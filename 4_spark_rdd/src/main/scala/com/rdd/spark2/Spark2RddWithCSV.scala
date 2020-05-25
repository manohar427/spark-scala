package com.rdd.spark2

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Spark2RddWithCSV {
  
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()

    val array = Array(3, 50000005, 7, 8, 33, 4, 4, 44, 55);

    //val arrayRdd = sparkSession.sparkContext.parallelize(array, 4);

    //val schema = StructType(StructField("Employee ID", IntegerType, true) :: Nil)
    //val rowRdd = arrayRdd.map(l => Row(l))
    val options = Map("header"-> "true","inferSchema"-> "true")
    val dataFrame = sparkSession.read.
        //option("header", true).
        //option("inferSchema", true).
          options(options).
          csv("D:\\Practice\\spark-scala\\data\\FL_insurance_sample.csv")

      dataFrame.printSchema();
      dataFrame.show();
  }  
}