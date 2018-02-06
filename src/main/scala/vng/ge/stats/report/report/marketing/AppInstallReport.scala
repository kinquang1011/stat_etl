package vng.ge.stats.report.report.marketing

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.render
import net.liftweb.json.Printer.compact
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.{JsonFormat, KpiFormat, MarketingFormat}
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import org.apache.spark.sql.functions._
import vng.ge.stats.report.writer.MarketingMySqlWriter

/**
  * Created by vinhdp on 3/15/17.
  */
class AppInstallReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
        
        if(Constants.Timing.A1 != timing) {
            
            Logger.info("\\_ App Install report only support A1 timing!")
            return false
        }
        
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
        val installDF = mpDF(Constants.LogTypes.Marketing.APPS_FLYER)
        val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
        val joinDF = installDF.join(newDF, installDF("userID") === newDF("id"), "left_outer")
        
        val resultDF = joinDF.groupBy("platform", "media_source", "campaign").agg(countDistinct("appsflyer_device_id").as('intall), countDistinct("userID").as('nru0))
        resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		/*var output = List[JsonFormat]()
		implicit val formats = net.liftweb.json.DefaultFormats
		
        val filter = df.where("platform is not null and media_source is not null")
        var mpMediaInstall = Map[String, Any]()
        var mpMediaNRU = Map[String, Any]()
        
        filter.collect().groupBy(row => row.getString(1))
            .map{ row =>
                // group by media source
                    val mediaSource = row._1
                    val values = row._2
                    var mpOsInstall = Map[String, Any]()
                    var mpOsNRU = Map[String, Any]()
                    
                    values.map { value =>
                        // map os values
                        val os = value.getString(0)
                        val install = value.getLong(2)
                        val nru0 = value.getLong(3)
                        
                        mpOsInstall += (os -> install)
                        mpOsNRU += (os -> nru0)
                    }
                
                    mpMediaInstall += (mediaSource -> mpOsInstall)
                    mpMediaNRU += (mediaSource -> mpOsNRU)
                }
        val installJson = compact(render(decompose(mpMediaInstall)))
        val nruJson = compact(render(decompose(mpMediaNRU)))
        
        output = JsonFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.MarketingKpi.INSTALL, timing), installJson) :: output
        output = JsonFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.MarketingKpi.NRU0, timing), nruJson) :: output
        */
        var output = List[MarketingFormat]()
        
		val filter = df.where("platform is not null and media_source is not null")
		filter.collect().foreach(row => {
			val os = row.getString(0)
			val mediaSource = row.getString(1)
			val campaign = row.getString(2)
			val install = row.getLong(3)
			val nru0 = row.getLong(4)
            
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.INSTALL, timing), install) :: output
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.NRU0, timing), nru0) :: output
		})
        
		val marWriter = new MarketingMySqlWriter(sparkSession)
		marWriter.write(output)
	}
}
