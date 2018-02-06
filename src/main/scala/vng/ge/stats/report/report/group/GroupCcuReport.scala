package vng.ge.stats.report.report.group

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}

/**
  * Created by vinhdp on 2/13/17.
  */
class GroupCcuReport(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		if(Constants.Timing.A1 != timing) {
			
			Logger.info("\\_ CCU report only support A1 timing!")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.CCU)){
			
			Logger.info("\\_ Skip group ccu report!")
			return false
		}
		
		Logger.info("\\_ Running group ccu report...")
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		
		val ccuDF = mpDF(Constants.LogTypes.CCU)
		
		val resultDF = ccuDF.select(groupId, "ccu")
			.withColumn(groupId, UDFs.makeOtherIfNull(col(groupId)))
			.groupBy(groupId).agg(avg("ccu"), max("ccu"))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGroupFormat]()
		
		df.collect().foreach { row =>
			
			val groupId = row.getString(0)
			val avg = row.getDouble(1)
			val max = row.getLong(2)
			
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.ACU, timing), avg) :: output
			output = KpiGroupFormat(source, gameCode, groupId, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.PCU, timing), max) :: output
			
			Logger.info("groupId: " + groupId + ", acu: " + avg + ", pcu: " + max, tab = 4)
		}
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
