package vng.ge.stats.etl.db

import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.transform.SparkEnv


/**
  * Created by canhtq on 28/06/2017.
  */
class ZeppDb {
  def getRs(): Unit ={
    val spark = SparkEnv.getSparkSession
    Class.forName("com.mysql.jdbc.Driver")
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://10.60.22.2/ubstats", "ubstats", "pubstats")
    val stmt = connection.prepareStatement("select * from game_kpi_hourly where report_date='2017-06-27' and game_code='ddd2mp2'")
    val rs: ResultSet = stmt.executeQuery()
    val json  = rs.getArray("kpi_value")
    val url="jdbc:mysql://10.60.22.2/ubstats"
    val prop = new java.util.Properties
    prop.setProperty("user","ubstats")
    prop.setProperty("password","pubstats")
    val kpi = spark.read.jdbc(url,"game_kpi_hourly",Array("report_date='2017-06-27'","game_code='ddd2mp2'"),prop)
    val active = spark.read.parquet("/ge/warehouse/3qmobile/ub/data/activity_2/2016-06-01").withColumn("hour",date_format(col("log_date"),"HH"))
    active.groupBy("hour").agg(countDistinct("id").as("active")).show


  }
}
