package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, sum}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/17/17.
  */
class NewUserRevenueReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.NEWUSER_REVENUE)) {
			
			Logger.info("Skip new user revenue report!")
			return false
		}
		
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
		val paymentDF = mpDF(Constants.LogTypes.PAYMENT)
		
		val joinDF = paymentDF.as('p).join(newDF.as('n),
			paymentDF("game_code") === newDF("game_code") && paymentDF(calcId) === newDF(calcId))
		
		val resultDF = joinDF.select("p.game_code", s"p.$calcId", Constants.NET_AMT, Constants.GROSS_AMT)
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
			
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING, timing), totalPaying) :: output
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_NET_REVENUE, timing), netRevenue) :: output
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_GROSS_REVENUE, timing), grossRevenue) :: output
			
			Logger.info("Game: " + gameCode + ", New user paying: " + totalPaying + ", net revenue: " + netRevenue + ", gross revenue: " + grossRevenue)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}