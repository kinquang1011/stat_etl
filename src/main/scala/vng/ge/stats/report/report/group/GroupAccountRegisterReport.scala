package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupAccountRegisterReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		if(!reportNumbers.contains(Constants.ReportNumber.ACCOUNT_REGISTER)){
			
			Logger.info("\\_ Skip group account register report!")
			return false
		}
		
		Logger.info("\\_ Running group account register report...")
		return true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		
		val registerDF = mpDF(Constants.LogTypes.ACC_REGISTER)
		
		val resultDF = registerDF.select(groupId, calcId).distinct
			.withColumn(groupId, UDFs.makeOtherIfNull(col(groupId)))
			.distinct.groupBy(groupId).agg(count(calcId))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			val groupId = row.getString(0)
			val totalReg = row.getLong(1)
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.ACCOUNT_REGISTER, timing), totalReg) :: output
			
			Logger.info("groupId: " + groupId + ", total reg: " + totalReg, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
