package vng.ge.stats.etl.utils

import java.text.SimpleDateFormat
import java.util.{Locale, Calendar}

import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Created by tuonglv on 27/12/2016.
 */
object Common {
    def touchFile(path: String, sc: SparkContext): Unit = {
        val data_arr = new Array[String](1)
        data_arr(0) = "flag"
        Try{
            sc.parallelize(data_arr).coalesce(1).saveAsTextFile(path)
        }
    }
    def logger(message: String, level: String = "INFO"): Unit ={
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val calendar = Calendar.getInstance(Locale.UK)
        val date = calendar.getTime
        val now = format.format(date)
        println(now + " UB-" + level + " " + message)
    }
}
