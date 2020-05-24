package com.rdd.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object Spark1Rdd {
  
  def main(args: Array[String]): Unit = {
    
    val sparkConfig = new SparkConf();
        sparkConfig.setAppName("First Spark example");
        sparkConfig.setMaster("local");

    val sparkContext = new SparkContext(sparkConfig);
    
    val sqlContext = new SQLContext(sparkContext);
    val array = Array(3, 50000005, 7, 8, 33, 4, 4, 44, 55);

    val arrayRdd = sparkContext.parallelize(array, 4);
    
    val schema = StructType(StructField("Employee ID",IntegerType,true)::Nil)
    val rowRdd  = arrayRdd.map(l=>Row(l))
        
     val dataFrame = sqlContext.createDataFrame(rowRdd,schema)    
     
     dataFrame.printSchema();
    
    dataFrame.show();
     
  }
}

