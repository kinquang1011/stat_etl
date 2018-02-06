package vng.ge.stats.report.report.marketing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.countDistinct
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.MarketingFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import vng.ge.stats.report.writer.MarketingMySqlWriter

/**
  * Created by vinhdp on 3/28/17.
  */
class FlyerInstallReport(sparkSession: SparkSession, parameters: Map[String, String])
    extends TReport(sparkSession, parameters) {
    
    override def validate(): Boolean = {
        
        if(Constants.Timing.A1 != timing) {
            
            Logger.info("\\_ App Install report only support A1 timing!")
            return false
        }
        
        return true
    }
    
    override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
        val totalDF = mpDF(Constants.LogTypes.Marketing.APPS_FLYER_TOTAL)
        
        val resultDF = totalDF.groupBy("platform", "media_source", "campaign")
            .agg(countDistinct("appsflyer_device_id").as('intall))
        resultDF
    }
    
    override def write(df: DataFrame): Unit = {
        var output = List[MarketingFormat]()
        
        df.collect().foreach(row => {
            val os = row.getString(0)
            val mediaSource = row.getString(1)
            val campaign = row.getString(2)
            val install = row.getLong(3)
            
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.INSTALL, timing), install) :: output
        })
        
        val marWriter = new MarketingMySqlWriter(sparkSession)
        marWriter.write(output)
    }
}
