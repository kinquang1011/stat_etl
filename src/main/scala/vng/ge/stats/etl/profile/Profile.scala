package vng.ge.stats.etl.profile
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.text.ParseException
import java.util.Locale
import java.util.ArrayList
import org.apache.spark.sql.types.{StructType, StructField, StringType};
/**
  * Created by canhtq on 08/05/2017.
  */
class Profile {
  case class User(
                   user_id: String
                    ,devices: scala.collection.immutable.Map[String,String]
                ,revenue:Long
                 ,total_login:Long
                 ,total_online:Long
                   ,total_pay:Long
                  )

    def make(): Unit ={
      val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.sql.orc.filterPushdown", "true").enableHiveSupport.getOrCreate()

      import spark.implicits._
      import spark.sql


    }
}
