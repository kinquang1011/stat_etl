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
class FlyerRegisterReport(sparkSession: SparkSession, parameters: Map[String, String])
    extends TReport(sparkSession, parameters) {
    
    override def validate(): Boolean = {
        
        if(Constants.Timing.A1 != timing) {
            
            Logger.info("\\_ App Install report only support A1 timing!")
            return false
        }
        
        return true
    }
    
    override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
        val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
            .selectExpr("platform", "media_source", "campaign", "user_id", "case when substring(install_time,0,10) == substring(reg_date,0,10) then user_id else null end as nru0_id")
        
        val resultDF = newDF.groupBy("platform", "media_source", "campaign").agg(countDistinct("user_id").as('nru), countDistinct("nru0_id").as('nru0))
        resultDF
    }
    
    override def write(df: DataFrame): Unit = {
        var output = List[MarketingFormat]()
        
        df.collect().foreach(row => {
            val os = row.getString(0)
            val mediaSource = row.getString(1)
            val campaign = row.getString(2)
            val nru1 = row.getLong(3)
            val nru0 = row.getLong(4)
            
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.NRU, timing), nru1) :: output
            output = MarketingFormat(source, gameCode, logDate, mediaSource, campaign, os, createDate,
                IdConfig.getKpiId(calcId, Constants.MarketingKpi.NRU0, timing), nru0) :: output
        })
        
        val marWriter = new MarketingMySqlWriter(sparkSession)
        marWriter.write(output)
    }
}
