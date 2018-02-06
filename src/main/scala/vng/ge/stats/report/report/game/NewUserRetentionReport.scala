package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/11/17.
  */
class NewUserRetentionReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(Constants.Timing.AC30 == timing || Constants.Timing.AC7 == timing){
			
			//Logger.info("New user retention can not run with timing AC7 & AC30")
			//return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.NEWUSER_RETENTION)) {
			
			//Logger.info("Skip new user retention report!")
			return false
		}
	
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
		val activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		
		val joinDF = newDF.as('n).join(activityDF.as('a), newDF(calcId) === activityDF(calcId), "left_outer").select("n." + calcId, "a." + calcId)
		val resultDF = joinDF.agg(count("n." + calcId), count("a." + calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		var rate = 0.0
		
		val newAcc = df.first().getLong(0)
		val retention = df.first().getLong(1)
		
		if(newAcc != 0) {
			rate = retention * 100.0 / newAcc
		}
		
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_USER_RETENTION_RATE, timing), rate) :: output
		Logger.info("New: " + newAcc + ", retention: " + retention)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
