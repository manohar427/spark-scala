package sparkrddcsv

import org.apache.spark.sql.SparkSession

object RDDWithCSVFile {

  def main(args: Array[String]): Unit = {
    val sparkSess = SparkSession.builder()
      .appName("SparkSession Example")
      .master("local")
      .getOrCreate()

    val rdd = sparkSess.sparkContext.textFile("D:\\Practice\\spark-scala\\data\\FL_insurance_sample.csv");
    
    //println("With Header:"+rdd.count())
    
    rdd.take(5).foreach(println)
    
    val headerRdd = rdd.first();
    
    //val csvRddWithOutHeader = rdd.filter(l => l!= headerRdd);
    
    val csvRddWithOutHeader = rdd.filter(_ != headerRdd);
    //println("With Out Header:"+csvRddWithOutHeader.count())
    
    csvRddWithOutHeader.take(5).foreach(println)
   
    //Take Two Columns and print it
    
    val twoColumnRdd = rdd.map(l => {
       val colArray = l.split(",");
          // List(colArray(0),colArray(1),colArray(2),colArray(3))
        //(colArray(0),colArray(1),colArray(2),colArray(3))
        Array(colArray(0),colArray(1),colArray(2),colArray(3)).mkString("=====")
    });
    
    twoColumnRdd.take(5).foreach(println);
    
    
  }
}