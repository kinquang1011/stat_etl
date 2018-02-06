package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions.{col, countDistinct, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupPaymentReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.REVENUE)) {
			
			Logger.info("\\_ Skip group payment report!")
			return false
		}
		
		Logger.info("\\_ Running group payment report...")
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		
		val paymentDF = mpDF(Constants.LogTypes.PAYMENT)
		
		val resultDF = paymentDF.select(groupId, calcId, Constants.NET_AMT, Constants.GROSS_AMT)
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
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.PAYING_USER, timing), totalPaying) :: output
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NET_REVENUE, timing), netRevenue) :: output
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.GROSS_REVENUE, timing), grossRevenue) :: output
			
			Logger.info("groupId: " + groupId + ", paying: " + totalPaying + ", net revenue: " + netRevenue + ", gross revenue: " + grossRevenue, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
