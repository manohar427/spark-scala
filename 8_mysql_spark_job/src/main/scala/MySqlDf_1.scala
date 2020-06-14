import org.apache.spark.sql.SparkSession
import java.util.Properties

object MySqlDf_1 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RDD with SparkSession Read CSV").master("local").getOrCreate()
    val url = "jdbc:mysql://localhost:3306"
    val table = "mysql.tutorials_tbl"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "test")
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlDF = sparkSession.read.jdbc(url, table, properties)
    mysqlDF.show()
  }
}