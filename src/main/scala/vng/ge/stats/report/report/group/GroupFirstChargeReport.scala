package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupFirstChargeReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.FIRST_CHARGE)) {
			
			Logger.info("\\_ Skip group first charge report!")
			return false
		}
		
		Logger.info("\\_ Running group first charge report...")
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		
		val paymentDF = mpDF(Constants.LogTypes.PAYMENT).select(groupId, calcId, Constants.NET_AMT, Constants.GROSS_AMT)
		val firstDF = mpDF(Constants.LogTypes.FIRST_CHARGE).select(calcId)
		
		val joinDF = paymentDF.join(firstDF,
			paymentDF(calcId) === firstDF(calcId), "leftsemi")    // do not need to join on group id because firstcharge is calculated on payment
		
		val resultDF = joinDF.select(groupId, calcId, Constants.NET_AMT, Constants.GROSS_AMT)
			.withColumn(groupId, UDFs.makeOtherIfNull(col(groupId)))
			.groupBy(groupId).agg(countDistinct(calcId), sum(Constants.NET_AMT), sum(Constants.GROSS_AMT))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			val groupId = row.getString(0)
			val totalPaying = row.getLong(1)
			val netRevenue = row.getDouble(2)
			val grossRevenue = row.getDouble(3)
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_PAYING, timing), totalPaying) :: output
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_PAYING_NET_REVENUE, timing), netRevenue) :: output
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_PAYING_GROSS_REVENUE, timing), grossRevenue) :: output
			
			Logger.info("groupId: " + groupId + ", new paying: " + totalPaying + ", net revenue: " + netRevenue + ", gross revenue: " + grossRevenue, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
