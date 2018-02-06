package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupFirstChargeRetentionReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(Constants.Timing.AC30 == timing || Constants.Timing.AC7 == timing){
			
			Logger.info("\\_ First charge retention can not run with timing AC7 & AC30")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.FIRST_CHARGE_RETENTION)) {
			
			Logger.info("\\_ Skip group first charge retention report!")
			return false
		}
		
		Logger.info("\\_ Running group first charge retention report...")
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		var paymentDF = mpDF(Constants.LogTypes.PAYMENT)
		var firstDF = mpDF(Constants.PREV + Constants.LogTypes.FIRST_CHARGE)

		firstDF = firstDF.select(calcId, groupId).distinct
		paymentDF = paymentDF.select(calcId, groupId).distinct
		
		val joinDF = firstDF.as('pf).join(paymentDF.as('cp),
			firstDF(calcId) === paymentDF(calcId) && firstDF(groupId) === paymentDF(groupId), "left_outer")
			.selectExpr("pf." + calcId + " as pf" + calcId, "cp." + calcId + " as cp" + calcId, "pf." + groupId)
		
		val resultDF = joinDF.withColumn(groupId, UDFs.makeOtherIfNull(joinDF(groupId)))
			.groupBy(groupId).agg(count("pf" + calcId), count("cp" + calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			var rate = 0.0
			val groupId = row.getString(0)
			val firstCharge = row.getLong(1)
			val retention = row.getLong(2)
			
			if(firstCharge != 0) {
				rate = retention * 100.0 / firstCharge
			}
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.RETENTION_PAYING_RATE, timing), rate) :: output
			
			Logger.info("groupId: " + groupId + ", first charge: " + firstCharge + ", retention: " + retention, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
