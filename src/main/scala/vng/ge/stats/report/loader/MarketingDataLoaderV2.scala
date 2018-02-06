package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.report.base.{DataPool, UDFs}
import vng.ge.stats.report.report.marketing._
import vng.ge.stats.report.util.Constants

/**
  * Created by vinhdp on 1/10/17.
  */
class MarketingDataLoaderV2(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var APPSFLYER_TOTAL_INSTALL = ""
	protected var APPSFLYER_REGISTER = ""
	
	override def readExtraParameter(): Unit = {
        
        APPSFLYER_TOTAL_INSTALL = parameters.getOrElse("appsflyer_file_name", "appsflyer/total_install")
        APPSFLYER_REGISTER = parameters.getOrElse("register_file_name", "appsflyer/new_register")
	}
	
	val mediaf = udf ((media:String)=> {
		var result = "Others";
		if (media != null) {
			result = media.trim.replace(" ", "").toLowerCase
			
		}
		result
	})
	
	override def run(): Unit = {
        
		val mappingDF = reader.option("header", "true").csv("/ge/fairy/warehouse/appsflyer/appsflyer_map.csv")
            .withColumn("src", mediaf(col("media_source")))
		
		/** required checking user id mapping **/
		val flyerTotal = reader.loadFile(gameCode, APPSFLYER_TOTAL_INSTALL, logDate, source, isHourly, logDir = logDir)
            .where(s"substring(install_time,0,10) = '$logDate'")
            .withColumn("src", mediaf(col("media_source")))
        
        val mFlyerTotal = flyerTotal.as('o)
            .join(mappingDF.as('m), flyerTotal("src") === mappingDF("src"), "left_outer")
            .drop("media_source", "src")
            .selectExpr("o.*", "m.source as media_source")
            .withColumn("media_source", UDFs.makeOthersIfNull(col("media_source")))
		
		val registerDF = reader.loadFile(gameCode, APPSFLYER_REGISTER, logDate, source, isHourly, logDir = logDir)
            .withColumn("src", mediaf(col("media_source"))).orderBy("install_time").dropDuplicates("user_id")
        
        val mRegisterDF = registerDF.as('o)
            .join(mappingDF.as('m), registerDF("src") === mappingDF("src"), "left_outer")
            .drop("media_source", "src")
            .selectExpr("o.*", "m.source as media_source")
            .withColumn("media_source", UDFs.makeOthersIfNull(col("media_source")))
        
		// using for calculating pu & rev
		val newDF = reader.loadFiles(gameCode, APPSFLYER_REGISTER, logDate, timing, source, isHourly, logDir = logDir)
            .withColumn("src", mediaf(col("media_source"))).orderBy("install_time").dropDuplicates("user_id")
        
        val mNewDF = newDF.as('o)
            .join(mappingDF.as('m), newDF("src") === mappingDF("src"), "left_outer")
            .drop("media_source", "src")
            .selectExpr("o.*", "m.source as media_source")
            .withColumn("media_source", UDFs.makeOthersIfNull(col("media_source")))
        
		val payDF = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, isHourly, logDir = logDir)
		
		/** calc app install **/
		new FlyerInstallReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.Marketing.APPS_FLYER_TOTAL -> mFlyerTotal
			)
		)
		
		/** calc app nru & nru0 **/
		new FlyerRegisterReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> mRegisterDF
			)
		)
		
		/** calc app pu & rev **/
		new FlyerRevenueReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> mNewDF,
				Constants.LogTypes.PAYMENT -> payDF
			)
		)
	}
}