package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import org.apache.spark.sql.functions.sum

/**
  * Created by vinhdp on 1/17/17.
  */
class PlayingTimeReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.PLAYING_TIME)) {
			
			Logger.info("Skip playing time report!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		val resultDF = activityDF.where("action = 'logout'").agg(sum("online_time"))
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		val time = df.first.getLong(0)
		
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.PLAYING_TIME, timing), time) :: output
		Logger.info("Total playing time: " + time)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}