package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupActiveUserReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!reportNumbers.contains(Constants.ReportNumber.ACTIVE_USER)){
			
			Logger.info("\\_ Skip group active user report!")
			return false
		}
		
		Logger.info("\\_ Running group active user report...")
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		
		val activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		
		val resultDF = activityDF.select(groupId, calcId).distinct
			.withColumn(groupId, UDFs.makeOtherIfNull(col(groupId)))
			.distinct.groupBy(groupId).agg(count(calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			val groupId = row.getString(0)
			val totalActive = row.getLong(1)
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.ACTIVE, timing), totalActive) :: output
			
			Logger.info("groupId: " + groupId + ", total active: " + totalActive, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
