package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 1/11/17.
  */
class GroupNewUserRetentionReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(Constants.Timing.AC30 == timing || Constants.Timing.AC7 == timing){
			
			Logger.info("\\_ New user retention can not run with timing AC7 & AC30")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.NEWUSER_RETENTION)) {
			
			Logger.info("\\_ Skip group new user retention report!")
			return false
		}
		
		Logger.info("\\_ Running group new user retention report...")
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		var newDF = mpDF(Constants.LogTypes.ACC_REGISTER)
		var activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		
		newDF = newDF.select(calcId, groupId).distinct
		activityDF = activityDF.select(calcId, groupId).distinct
		
		val joinDF = newDF.as('pr).join(activityDF.as('ca),
			newDF(calcId) === activityDF(calcId) && newDF(groupId) === activityDF(groupId), "left_outer")
			.selectExpr("pr." + calcId + " as pr" + calcId, "ca." + calcId + " as ca" + calcId, "pr." + groupId)
		
		val resultDF = joinDF.withColumn(groupId, UDFs.makeOtherIfNull(joinDF(groupId)))
			.groupBy(groupId).agg(count("pr" + calcId), count("ca" + calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			var rate = 0.0
			val groupId = row.getString(0)
			val newAcc = row.getLong(1)
			val retention = row.getLong(2)
			
			if(newAcc != 0) {
				rate = retention * 100.0 / newAcc
			}
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.NEW_USER_RETENTION_RATE, timing), rate) :: output
			
			Logger.info("groupId: " + groupId + ", new: " + newAcc + ", retention: " + retention + ", rate: " + rate, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
