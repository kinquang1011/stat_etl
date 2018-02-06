package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import vng.ge.stats.report.base.{DataPool, UDFs}
import vng.ge.stats.report.report.game._
import vng.ge.stats.report.report.marketing.{AppInstallReport, AppsFlyerRegisterReport, AppsFlyerRevenueReport}
import vng.ge.stats.report.util.{Constants, DateTimeUtils}

/**
  * Created by vinhdp on 1/10/17.
  */
class MarketingDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var APPSFLYER = ""
	
	override def readExtraParameter(): Unit = {
		
		APPSFLYER = parameters.getOrElse("appsflyer_file_name", "appsflyer/solu4")
	}
	
	override def run(): Unit = {
        
		val mappingDF = reader.option("header", "true").csv("/ge/fairy/warehouse/appsflyer/appsflyer_map.csv")
		
		/** required checking user id mapping **/
		val appsflyer = reader.loadFile(gameCode, APPSFLYER, logDate, source, isHourly, logDir = logDir)
        val mappsFlyer = appsflyer.as('o)
            .join(mappingDF.as('m), appsflyer("media_source") === mappingDF("media_source"), "left_outer")
            .drop("media_source")
            .selectExpr("o.*", "m.source as media_source")
            .withColumn("media_source", UDFs.makeOthersIfNull(col("media_source")))
		
		val accregisterDF = reader.loadFile(gameCode, Files.ACC_REGISTER, logDate, source, isHourly, logDir = logDir)
		
		/** need to combine multi file into one for performance**/
		val totalAppsFlyer = reader.loadFile(gameCode, APPSFLYER, "*", source, isHourly, logDir = logDir)
        val mtotalAppsFlyer = totalAppsFlyer.as('o)
            .join(mappingDF.as('m), totalAppsFlyer("media_source") === mappingDF("media_source"), "left_outer")
            .drop("media_source")
            .selectExpr("o.*", "m.source as media_source")
            .withColumn("media_source", UDFs.makeOthersIfNull(col("media_source")))
        
		// using for calculating pu & rev
		val newDF = reader.loadFiles(gameCode, Files.ACC_REGISTER, logDate, timing, source, isHourly, logDir = logDir)
		val payDF = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, isHourly, logDir = logDir)
		
		/** calc app install & nru0 **/
		new AppInstallReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.Marketing.APPS_FLYER -> mappsFlyer,
				Constants.LogTypes.ACC_REGISTER -> accregisterDF
			)
		)
		
		/** calc app nru **/
		new AppsFlyerRegisterReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.Marketing.APPS_FLYER -> mtotalAppsFlyer,
				Constants.LogTypes.ACC_REGISTER -> accregisterDF
			)
		)
		
		/** calc app pu & rev **/
		new AppsFlyerRevenueReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.Marketing.APPS_FLYER -> mtotalAppsFlyer,
				Constants.LogTypes.ACC_REGISTER -> newDF,
				Constants.LogTypes.PAYMENT -> payDF
			)
		)
	}
}