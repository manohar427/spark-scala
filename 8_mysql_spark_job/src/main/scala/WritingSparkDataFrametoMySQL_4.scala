import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode

object WritingSparkDataFrametoMySQL_4 {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val url = "jdbc:mysql://localhost:3306"
    // val table = "mysql.tutorials_tbl"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "test")
    Class.forName("com.mysql.jdbc.Driver")

    val options = Map("header" -> "true", "inferSchema" -> "true")

    val dataFrame = sparkSession.read.options(options).csv("D:\\Practice\\spark-scala\\data\\CustomerVehicle2.csv")

    /* val table = "mysql.customers"
    	val mysqlDF = dataFrame.write.mode(SaveMode.Overwrite).jdbc(url, table, properties)
			*/

    //Check CSV data count
    println(dataFrame.count())

  }
}