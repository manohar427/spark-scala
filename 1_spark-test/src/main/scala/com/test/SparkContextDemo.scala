package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextDemo {
  
  def main(args: Array[String]): Unit = {
  
    val sparkConfig = new SparkConf();
    sparkConfig.setAppName("First Spark example");
    sparkConfig.setMaster("local");

    val sparkContext = new SparkContext(sparkConfig);
    val array = Array(3, 55, 7, 8, 33, 4, 4, 44, 55);

    val arrayRdd = sparkContext.parallelize(array, 4);

    println("No of elements in RDD::" + arrayRdd.count());

    arrayRdd.foreach(println);

    val fileData = "D:/Practice/spark-scala/data/FL_insurance_sample.csv";

    val fileRdd = sparkContext.textFile(fileData, 5);

    println("File rdd/row count:" + fileRdd.count());

    println("First Row:" + fileRdd.first());

  }
}