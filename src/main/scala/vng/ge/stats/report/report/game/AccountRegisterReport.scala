package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/16/17.
  */
class AccountRegisterReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		if(!reportNumbers.contains(Constants.ReportNumber.ACCOUNT_REGISTER)){
			
			Logger.info("Skip account register report!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val registerDF = mpDF(Constants.LogTypes.ACC_REGISTER)
		val resultDF = registerDF.select(s"$calcId").distinct
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		val register = df.count
		
		output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.ACCOUNT_REGISTER, timing), register) :: output
		Logger.info("Account register: " + register)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}