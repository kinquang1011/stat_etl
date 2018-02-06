package vng.ge.stats.report.test

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.model.{KpiFormat, KpiGroupFormat}
import vng.ge.stats.report.util._

/**
  * Created by vinhdp on 1/9/17.
  */
object Tester {
	
	def main(args: Array[String]) {
		
		
		//val sparkSession = SparkSession.builder.master(Constants.Default.MASTER).appName("123").getOrCreate()
		//Logger.info("vinhdp")
		
		//var output = List[KpiGroupFormat]()

		//output = KpiGroupFormat("", "", "", "", "", IdConfig.getKpiId("id", Constants.Kpi.NEW_USER_RETENTION_RATE, "a1"), 0.0) :: output
		//println(output.head.getClass.getSimpleName)
		
		//var output2 = List[KpiFormat]()
		
		//output2 = KpiFormat("", "", "", "", IdConfig.getKpiId("id", Constants.Kpi.NEW_USER_RETENTION_RATE, "a1"), 0.0) :: output2
		
		//println(output2.head.getClass.getSimpleName)
		
		//val test: List[Any] = output
		//write(test)
		
		//val date = ZonedDateTime.now(ZoneId.of("GMT")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
		//val date = DateTimeUtils.getTimingStartDate("2017-01-10", "ac7")
		//println(date)
		//println(PathUtils.getFilePatterns("/ge/warehouse/jxm/payment_2", "2017-01-10", "a3"))
		//println(PathUtils.makePath(Constants.Default.LOG_DIR, "jxm", "activity_2"))
		
		option("name", "vinhdp")
		option("age", "23")
		
		val opts = scala.collection.Map("s" -> "1", "r" -> "no")
		
		options(opts)
		
		for((key, value) <- extraOptions){
			println(value)
		}
		
		println(DateTimeUtils.getTimingStartDate("2017-01-31", "a30"))
	}
	
	var extraOptions = new scala.collection.mutable.HashMap[String, String]
	
	def option(key: String, value: String) = {
		this.extraOptions += (key -> value)
	}
	def options(options: scala.collection.Map[String, String]) = {
		this.extraOptions ++= options
	}
	
	def write(data: List[Any]): Unit = {
		
		if(!data.isEmpty) {
			
			val className = data.head.getClass.getSimpleName
			className match {
				case "KpiFormat" => writeGameReport(data.asInstanceOf[List[KpiFormat]])
				case "KpiGroupFormat" => writeGroupReport(data.asInstanceOf[List[KpiGroupFormat]])
			}
			
		} else {
			Logger.warning("Result is empty! Nothing to write!")
		}
	}
	
	def writeGameReport(data: List[KpiFormat]): Unit = {
		println("write game data format")
	}
	
	def writeGroupReport(data: List[KpiGroupFormat]): Unit = {
		println("write group data format")
	}
}
