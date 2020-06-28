import org.apache.spark.sql.SparkSession
import java.util.Properties

import org.apache.spark.sql.functions.{min,max,avg}
import org.apache.spark.sql.functions.{upper,lower,length}

object Built_In_Function_1 {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()

    val options = Map("header" -> "true", "inferSchema" -> "true")

    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle2.csv")
    
   dataFrame.select(lower(dataFrame.col("Country")),upper(dataFrame.col("Country")),length(dataFrame.col("Country"))).show()
   // dataFrame.foreach(l =>println(l))
    //dataFrame.show()
  }
}