package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by canhtq on 30/05/2017.
  */
class TotalNRUReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		if(!reportNumbers.contains(Constants.ReportNumber.TOTAL_NRU)){
			
			Logger.info("Skip TOTAL_NRU report!")
			return false
		}
		if(Constants.Timing.A1 != timing) {

			Logger.info("TOTAL_NRU only support A1 timing!")
			return false
		}
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val totalDF = mpDF(Constants.LogTypes.TOTAL_LOGIN_ACC)
		val resultDF = totalDF.select(s"$calcId").distinct
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		val register = df.count
		
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.TOTAL_NRU, timing), register) :: output
		Logger.info("TOTAL_NRU: " + register)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}