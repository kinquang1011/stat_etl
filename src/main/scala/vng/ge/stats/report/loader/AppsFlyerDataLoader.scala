package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.report.marketing.{AppInstallReport, AppsFlyerRegisterReport, AppsFlyerRevenueReport}
import vng.ge.stats.report.util.{Constants, DateTimeUtils}
import org.apache.spark.sql.functions._

/**
  * Created by vinhdp on 1/10/17.
  */
class AppsFlyerDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var APPSFLYER = ""
	
	override def readExtraParameter(): Unit = {
		
		APPSFLYER = parameters.getOrElse("appsflyer_file_name", "appsflyer/solu3")
	}
	
	override def run(): Unit = {
        
		val appsflyer = reader.loadFile(gameCode, APPSFLYER, logDate, source, isHourly, logDir = logDir)
		
		val newDF = reader.loadFile(gameCode, Files.ACC_REGISTER, logDate, source, isHourly, logDir = logDir)
        //load 7 day payment
		val payDF = reader.loadFiles(gameCode, Files.PAYMENT, DateTimeUtils.addDate(logDate, "a7"), "a7", source, isHourly, logDir = logDir)
		
		val payn = payDF.as('p).join(newDF.as('n), payDF("id") === newDF("id")).select("p.*")
		
		val p = payn.selectExpr("id", "cast (net_amt as long) as net_amt").groupBy("id").agg(sum("net_amt").as("total"))
		
		val joinDF = appsflyer.as('a).join(p.as('p), appsflyer("userID") === p("id"), "left_outer")
		
		val results = joinDF.groupBy("platform", "media_source").agg(count("platform").as("install"), sum("total").as("rev7")).withColumn("log_date", lit(logDate))
            .select("log_date", "platform", "media_source", "install", "rev7")
		results.coalesce(1).write.format("csv").mode("overwrite").save(s"/ge/warehouse/cack/ub/sdk_data/flyer_result/$logDate")
	}
}