package vng.ge.stats.report.test

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.google.common.base.Strings
import org.apache.spark.sql.functions.{coalesce, col}
import vng.ge.stats.report.util.Constants

import scala.reflect.macros.whitebox

/**
  * Created by vinhdp on 1/16/17.
  */
object TestF {
	
	case class Student(name: String, score: Int)
	
	def main(args: Array[String]) {
		
		val hannah = Student("Hannah", 95)
		
		val vari = 10.1
		
		println(s"${hannah.score + 1} Variables: %09.04f".format(vari))
		println()
		
		val jdbcUsername = "ubstats"
		val jdbcPassword = "pubstats"
		val jdbcHostname = "10.60.22.2"
		val jdbcPort = 3306
		val jdbcDatabase ="vinhdp"
		val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
		val connectionProperties = new java.util.Properties()
		connectionProperties.put("driver", "com.mysql.jdbc.Driver")
	}
	
/*val incrementTimeZone = udf {(date: String) =>
var newDateTime = ""
Try {
val timeStamp = DateTimeUtils.getTimestamp(date)
newDateTime = DateTimeUtils.getDate(timeStamp + 25200 * 1000)
}
newDateTime
}

def changeDate(fileName: String, fromDate: String, toDate: String): Unit = {
var start = LocalDate.parse(fromDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT, Locale.UK))
val end = LocalDate.parse(toDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT, Locale.UK))
var prevDate = "2017-02-08"

while(!start.isAfter(end)) {
val date = start.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
val file = s"/user/fairy/vinhdp/nikkisea/sdk_data/$fileName/{$date,$prevDate}"

val parquetFile = spark.read.parquet(file)
val changeFile = parquetFile.withColumn("log_date", incrementTimeZone(col("log_date")))
changeFile.where(s"substring(log_date,0,10) = '$date'").coalesce(1).write.mode("overwrite").format("parquet").save(s"/ge/warehouse/nikkisea/ub/sdk_data/$fileName/$date")
println(s"transfering file $fileName log date: $date  prev date: $prevDate")
prevDate = date
start = start.plusDays(1)
}
}*/
}
