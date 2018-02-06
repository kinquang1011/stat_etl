package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.count
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/16/17.
  */
class UserRetentionReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(Constants.Timing.AC30 == timing || Constants.Timing.AC7 == timing){
			
			//Logger.info("User retention can not run with timing AC7 & AC30")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.USER_RETENTION)) {
			
			//Logger.info("Skip user retention report!")
			return false
		}
		
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val prevActivityDF = mpDF(Constants.PREV + Constants.LogTypes.ACTIVITY)
		val activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		
		val joinDF = prevActivityDF.as('pa).join(activityDF.as('ca), prevActivityDF(calcId) === activityDF(calcId), "left_outer").select("pa." + calcId, "ca." + calcId)
		val resultDF = joinDF.agg(count("pa." + calcId), count("ca." + calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		var rate = 0.0
		
		val prevActive = df.first().getLong(0)
		val retention = df.first().getLong(1)
		
		if(prevActive != 0) {
			rate = retention * 100.0 / prevActive
		}
		
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.USER_RETENTION_RATE, timing), rate) :: output
		Logger.info("Prev Active: " + prevActive + ", retention: " + retention)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
