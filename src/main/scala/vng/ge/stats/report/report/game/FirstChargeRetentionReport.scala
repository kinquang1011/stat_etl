package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.model.KpiFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/17/17.
  */
class FirstChargeRetentionReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(Constants.Timing.AC30 == timing || Constants.Timing.AC7 == timing){
			
			Logger.info("First charge retention can not run with timing AC7 & AC30")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.FIRST_CHARGE_RETENTION)) {
			
			Logger.info("Skip first charge retention report!")
			return false
		}
		
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val firstDF = mpDF(Constants.PREV + Constants.LogTypes.FIRST_CHARGE)
		val paymentDF = mpDF(Constants.LogTypes.PAYMENT)
		
		val joinDF = firstDF.as('f).join(paymentDF.as('p),
			firstDF("game_code") === paymentDF("game_code") && firstDF(calcId) === paymentDF(calcId), "left_outer")
			.select("f.game_code", s"f.$calcId", s"p.$calcId")
		val resultDF = joinDF.groupBy("f.game_code").agg(countDistinct(s"f.$calcId"), countDistinct(s"p.$calcId"))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiFormat]()
		df.collect().foreach { row =>
			
			var rate = 0.0
			val gameCode = row.getString(0)
			val totalFirst = row.getLong(1)
			val totalRetention = row.getLong(2)
			
			if (totalFirst != 0) {
				rate = totalRetention * 100.0 / totalFirst
			}
			output = KpiFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.RETENTION_PAYING_RATE, timing), rate) :: output
			Logger.info("Game: " + gameCode + ", first charge: " + totalFirst + ", retention: " + totalRetention)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}