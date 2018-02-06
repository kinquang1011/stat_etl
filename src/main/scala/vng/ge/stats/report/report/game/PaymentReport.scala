package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import org.apache.spark.sql.functions._

/**
  * Created by vinhdp on 1/16/17.
  */
class PaymentReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.REVENUE)) {
			
			Logger.info("Skip payment report!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val paymentDF = mpDF(Constants.LogTypes.PAYMENT)
		val resultDF = paymentDF.filter("game_code is not null")
			.groupBy("game_code").agg(sum(Constants.NET_AMT), sum(Constants.GROSS_AMT), countDistinct(calcId))
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		df.collect().foreach { row =>
			val gameCode = row.getString(0)
			val netRevenue = row.getDouble(1)
			val grossRevenue = row.getDouble(2)
			val pu = row.getLong(3)
			
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.PAYING_USER, timing), pu) :: output
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NET_REVENUE, timing), netRevenue) :: output
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.GROSS_REVENUE, timing), grossRevenue) :: output
			
			Logger.info("Game: " + gameCode + ", Total: " + pu + ", net revenue: " + netRevenue + ", gross revenue: " + grossRevenue)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
