package vng.ge.stats.report.report.game

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import vng.ge.stats.report.base.{TReport, UDFs}
import vng.ge.stats.report.model.KpiGameRetentionFormat
import vng.ge.stats.report.util.{Constants, IdConfig, Logger}
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._

/**
  * Created by vinhdp on 1/17/17.
  */
class GameRetentionReport (sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def validate(): Boolean = {
		
		if(!(Constants.Timing.A1 == timing || Constants.Timing.AC7 == timing || Constants.Timing.AC30 == timing)){
			
			Logger.info("Game retention only run with timing A1, AC7 & AC30")
			return false
		}
		
		if(!reportNumbers.contains(Constants.ReportNumber.GAME_RETENTION)) {
			
			Logger.info("Skip game retention report!")
			return false
		}
		
		true
	}
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		val totalDF = mpDF(Constants.LogTypes.TOTAL_LOGIN_ACC)
		val activityDF = mpDF(Constants.LogTypes.ACTIVITY)
		
		val userDF = activityDF.select(UDFs.getTimePeriod(col("log_date"), lit(timing)).as('log_date), col(calcId)).distinct()
		
		var totalAccDF = totalDF.where("log_date <= '" + logDate + " 24:00:00'")
		totalAccDF = totalAccDF.select(UDFs.getTimePeriod(col("log_date"), lit(timing)).as('log_date), col(calcId))
		
		val retentionDF = userDF.as('u).join(
			totalAccDF.as('t), userDF(calcId) === totalAccDF(calcId))
			.selectExpr("t.log_date as reg_date", "u.log_date", "u." + calcId)
		val resultDF = retentionDF.groupBy("reg_date", "log_date").agg(count(calcId).as('retention))
		
		resultDF
	}
	
	override def write(df: DataFrame): Unit = {
		var output = List[KpiGameRetentionFormat]()
		var jsonObj = Map[String, Any]()
		implicit val formats = net.liftweb.json.DefaultFormats
		
		df.collect().foreach { row => {
			
				val regDate = row.getAs[String]("reg_date")
				val retention = row.getAs[Long]("retention")
				
				jsonObj += (regDate -> retention)
			}
		}
		
		val json = compact(render(decompose(jsonObj)))
		
		output = KpiGameRetentionFormat(source, gameCode, logDate, createDate, IdConfig.getKpiId(calcId, Constants.Kpi.USER_RETENTION, timing), json) :: output
		
		Logger.info("Retention: " + json)
		
		writer.format(Constants.DataSources.JDBC).write(output)
	}
}
