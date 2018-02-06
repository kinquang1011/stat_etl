package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/16/17.
  */
class FirstChargeReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.FIRST_CHARGE)) {
			
			Logger.info("Skip first charge report!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val firstChargeDF = mpDF(Constants.LogTypes.FIRST_CHARGE)
		val paymentDF = mpDF(Constants.LogTypes.PAYMENT)
		
		val joinDF = paymentDF.as('p).join(firstChargeDF.as('f),
			paymentDF("game_code") === firstChargeDF("game_code") && paymentDF(calcId) === firstChargeDF(calcId), "leftsemi")
		
		val resultDF = joinDF.select("game_code", calcId, Constants.NET_AMT, Constants.GROSS_AMT)
			.coalesce(1).groupBy("game_code").agg(countDistinct(calcId), sum(Constants.NET_AMT), sum(Constants.GROSS_AMT))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		df.collect().foreach { row =>
			val gameCode = row.getString(0)
			val totalPaying = row.getLong(1)
			val netRevenue = row.getDouble(2)
			val grossRevenue = row.getDouble(3)
			
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_PAYING, timing), totalPaying) :: output
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_PAYING_NET_REVENUE, timing), netRevenue) :: output
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_PAYING_GROSS_REVENUE, timing), grossRevenue) :: output
			
			Logger.info("Game: " + gameCode + ", first charge: " + totalPaying + ", revenue: " + netRevenue + ", gross revenue: " + grossRevenue)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
