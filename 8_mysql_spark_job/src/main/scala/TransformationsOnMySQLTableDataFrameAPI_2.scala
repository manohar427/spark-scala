import org.apache.spark.sql.SparkSession
import java.util.Properties

object TransformationsOnMySQLTableDataFrameAPI_2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val url = "jdbc:mysql://localhost:3306"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "test")
    Class.forName("com.mysql.jdbc.Driver")
    val query = "select * from mysql.tutorials_tbl"
    val mysqlDF = sparkSession.read.jdbc(url, s"($query) as cust_table", properties)
    mysqlDF.show()
  }
}