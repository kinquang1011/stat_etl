package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import org.apache.spark.sql.functions._

/**
  * Created by vinhdp on 1/16/17.
  */
class CcuReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		if(Constants.Timing.A1 != timing) {
			
			Logger.info("CCU report only support A1 timing!")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.CCU)){
			
			Logger.info("Skip ccu report!")
			return false
		}
		
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val ccuDF = mpDF(Constants.LogTypes.CCU)
		val resultDF = ccuDF.groupBy("log_date").agg(sum("ccu").as('ccu)).agg(avg("ccu"), max("ccu"))
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		val avg = df.first.getDouble(0)
		val max = df.first.getLong(1)
		
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.ACU, timing), avg) :: output
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.PCU, timing), max) :: output
		Logger.info("Game: " + gameCode + ", avg: " + avg + ", max: " + max)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
