package com.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object DataFrameUsingSparkSession {
  
  def main(args: Array[String]): Unit = {
    val sparkSess = SparkSession.builder()
        .appName("SparkSession Example")
       .master("local")
        .getOrCreate()
    
       val rdd = sparkSess.sparkContext.parallelize(Array(1,4,7,9,2,5,1,8));
       val schema = StructType(StructField("Integers data",IntegerType,true)::Nil)
       
       val rowRdd = rdd.map(e=>Row(e))
       
       val df = sparkSess.createDataFrame(rowRdd, schema);
       
         df.printSchema()
         df.show(3)
    
  }
}