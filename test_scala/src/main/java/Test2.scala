import org.apache.spark.sql.SparkSession
import java.util.Properties

object Test2 extends App {

//  println("ok.........")

  val spark = SparkSession.builder()

    .appName("Cloudera Sample Job")
    .master("yarn-client")
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.0.5:8020")
    .config("spark.hadoop.yarn.resourcemanager.address", "192.168.0.5:8032")
    .config("spark.yarn.jars", "hdfs://192.168.0.5:8020/user/DELL/jars/*.jar")
    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
    .getOrCreate()
     println("=======creation completed===========")
     /*var location = "/user/DELL/data/CustomerVehicle.csv";
      var df = spark.read.option("header", "true")
      .csv(location)
      
      df.show();*/
     
  val url = "jdbc:mysql://192.168.0.5:3306"
  val table = "retail_db.customers";
  val prop = new Properties();
  prop.setProperty("user", "root");
  prop.setProperty("password", "cloudera");
  Class.forName("com.mysql.jdbc.Driver")

  val mysql =   spark.read.jdbc(url, table, prop)
  mysql.show();
     
  println("==================")
}