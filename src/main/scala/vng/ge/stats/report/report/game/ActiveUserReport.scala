package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/16/17.
  */
class ActiveUserReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.ACTIVE_USER)){
			
			Logger.info("Skip active user report!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		val resultDF = activityDF.select(s"$calcId").distinct
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		val active = df.count

		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.ACTIVE, timing), active) :: output
		Logger.info("Active: " + active)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
