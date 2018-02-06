package vng.ge.stats.report.report.marketing

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.render
import net.liftweb.json.Printer.compact
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.{JsonFormat, KpiFormat, MarketingFormat}
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import vng.ge.stats.report.writer.MarketingMySqlWriter

/**
  * Created by vinhdp on 3/15/17.
  */
class AppsFlyerRegisterReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		if(Constants.Timing.A1 != timing) {
			
			Logger.info("\\_ App Register report only support A1 timing!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
        
        val totalInstallDF = mpDF(Constants.LogTypes.Marketing.APPS_FLYER)
        val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
        
        val joinDF = newDF.join(totalInstallDF, newDF("id") === totalInstallDF("userID"), "left_outer")
        
        val resultDF = joinDF.groupBy("platform", "media_source", "campaign").agg(countDistinct("id").as('nru))
        resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[MarketingFormat]()
		
		val filter = df.where("platform is not null and media_source is not null")
		filter.collect().foreach(row => {
			val os = row.getString(0)
			val mediaSource = row.getString(1)
			val campaign = row.getString(2)
			val nru = row.getLong(3)
			
			output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
				IdConfig.getKpiId(calcId, Constants.MarketingKpi.NRU, timing), nru) :: output
		})
		
		val marWriter = new MarketingMySqlWriter(sparkSession)
		marWriter.write(output)
	}
}
