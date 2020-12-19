package com.test

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val sparkSess = SparkSession.builder()
      .appName("Spark Example with GCP")
      //.master("local")
      .getOrCreate()
    val rdd = sparkSess.sparkContext.parallelize(Array(1,4,7,9,2,5,1,8));
    rdd.collect().foreach(println)

  }
}
