package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupUserRetentionReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(Constants.Timing.AC30 == timing || Constants.Timing.AC7 == timing){
			
			Logger.info("\\_ User retention can not run with timing AC7 & AC30")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.USER_RETENTION)) {
			
			Logger.info("\\_ Skip group user retention report!")
			return false
		}
		
		Logger.info("\\_ Running group user retention report...")
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		
		var prevActivityDF = mpDF(Constants.PREV + Constants.LogTypes.ACTIVITY)
		var activityDF = mpDF(Constants.LogTypes.ACTIVITY)
			
		prevActivityDF = prevActivityDF.select(calcId, groupId).distinct
		activityDF = activityDF.select(calcId, groupId).distinct
		
		val joinDF = prevActivityDF.as('pa).join(activityDF.as('ca),
			prevActivityDF(calcId) === activityDF(calcId) && prevActivityDF(groupId) === activityDF(groupId), "left_outer")
			.selectExpr("pa." + calcId + " as pa" + calcId, "ca." + calcId + " as ca" + calcId, "pa." + groupId)
		
		val resultDF = joinDF.select(groupId, "pa" + calcId, "ca" + calcId)
			.withColumn(groupId, UDFs.makeOtherIfNull(col(groupId)))
			.groupBy(groupId).agg(count("pa" + calcId), count("ca" + calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			var rate = 0.0
			val groupId = row.getString(0)
			val totalAcc = row.getLong(1)
			val retention = row.getLong(2)
			
			if(totalAcc != 0) {
				rate = retention * 100.0 / totalAcc
			}
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.USER_RETENTION_RATE, timing), rate) :: output
			
			Logger.info("groupId: " + groupId + ", total: " + totalAcc + ", retention: " + retention + ", rate: " + rate, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
