package vng.ge.stats.report.report.marketing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, sum}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.MarketingFormat
import vng.ge.stats.report.util.{Constants, IdConfig}
import vng.ge.stats.report.writer.MarketingMySqlWriter

/**
  * Created by vinhdp on 3/28/17.
  */
class FlyerRevenueReport(sparkSession: SparkSession, parameters: Map[String, String])
    extends TReport(sparkSession, parameters) {
    
    override def validate(): Boolean = {
        
        return true
    }
    
    override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
        
        val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
        val payDF = mpDF(Constants.LogTypes.PAYMENT)
        
        val userPayDF = payDF.groupBy("id").agg(sum("net_amt").as('amt))
        val joinDF = newDF.as('n).join(userPayDF.as('p), newDF("user_id") === userPayDF("id")).selectExpr("substring(n.reg_date, 0, 10) as log_date", "media_source", "platform", "campaign", "n.user_id", "p.amt")
        
        val resultDF = joinDF.groupBy("log_date", "platform", "media_source", "campaign").agg(count("user_id").as('pu), sum("amt").as('amt))
        resultDF
    }
    
    override def write(df: DataFrame): Unit = {
        
        var output = List[MarketingFormat]()
        
        df.collect().foreach(row => {
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
