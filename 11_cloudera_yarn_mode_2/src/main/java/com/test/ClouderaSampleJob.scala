package com.test

import org.apache.spark.sql.SparkSession

object ClouderaSampleJob extends App{
  val spark = SparkSession.builder()
    .master("yarn")
    //or try with yarn-client
   .appName("Cloudera Sample Job")
  //.config("spark.hadoop.fs.defaultFS","hdfs://192.168.0.5:8020")
  //.config("spark.hadoop.yarn.resourcemanager.address","192.168.0.5:8032")
  .config("spark.yarn.jars","hdfs://192.168.0.5:8020/user/DELL/jars/*.jar")
 // .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
  .getOrCreate()
  
  val csvData = spark.read.option("header", "true").csv("D:/Practice/spark-scala/11_cloudera_yarn_mode_2/src/main/resources/SacramentocrimeJanuary2006.csv")
  csvData.show();
  println("==================")
}