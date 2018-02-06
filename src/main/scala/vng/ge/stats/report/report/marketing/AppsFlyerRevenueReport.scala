package vng.ge.stats.report.report.marketing

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.render
import net.liftweb.json.Printer.compact
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.{JsonFormat, KpiFormat, MarketingFormat}
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import vng.ge.stats.report.writer.{MarketingMySqlWriter, Writer}

/**
  * Created by vinhdp on 3/15/17.
  */
class AppsFlyerRevenueReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
        
        val totalInstallDF = mpDF(Constants.LogTypes.Marketing.APPS_FLYER)
        val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
		val payDF = mpDF(Constants.LogTypes.PAYMENT)
        
        val totalDF = totalInstallDF.orderBy("userID", "install_time").dropDuplicates("userID")
        val userPayDF = payDF.groupBy("id").agg(sum("net_amt").as('amt))
        
        val registerDF = newDF.as('n).join(totalDF.as('t), newDF("id") === totalDF("userID"), "left_outer").selectExpr("substring(n.log_date, 0, 10) as log_date", "t.platform", "t.media_source", "t.campaign", "n.id")
        
        val joinDF = registerDF.as('r).join(userPayDF.as('p), registerDF("id") === userPayDF("id")).select("log_date", "media_source", "platform", "campaign", "r.id", "p.amt")
        
        val resultDF = joinDF.groupBy("log_date", "platform", "media_source", "campaign").agg(count("id").as('pu), sum("amt").as('amt))
        resultDF
	}
	
	override def write(df: DataFrame): Unit = {
        
        var output = List[MarketingFormat]()
        val filter = df.where("platform is not null and media_source is not null")
        
        filter.collect().foreach(row => {
            val logDate = row.getString(0)
            val os = row.getString(1)
            val mediaSource = row.getString(2)
            val campaign = row.getString(3)
            val pu = row.getLong(4)
            val rev = row.getDouble(5)
            
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.PU, timing), pu) :: output
    
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.REV, timing), rev) :: output
        })
        
        val marWriter = new MarketingMySqlWriter(sparkSession)
        marWriter.write(output)
	}
}
