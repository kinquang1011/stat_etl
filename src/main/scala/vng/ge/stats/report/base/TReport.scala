package vng.ge.stats.report.base

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.util.{Constants, Logger}

/**
  * Created by vinhdp on 1/9/17.
  */
abstract class TReport(sparkSession: SparkSession, parameters: Map[String, String]) {
	
	protected val reader = new DataReader(sparkSession)
	protected val writer = new DataWriter(sparkSession, parameters)
	
	protected var gameCode = parameters(Constants.Parameters.GAME_CODE)
	protected var logDate = parameters(Constants.Parameters.LOG_DATE)
	protected var timing = parameters.getOrElse(Constants.Parameters.TIMING, Constants.Default.TIMING)
	protected var calcId = parameters.getOrElse(Constants.Parameters.CALC_ID, Constants.Default.CALC_ID)
	protected var source = parameters.getOrElse(Constants.Parameters.SOURCE, Constants.Default.SOURCE)
	protected var groupId = parameters.getOrElse(Constants.Parameters.GROUP_ID, Constants.Default.EMPTY_STRING)
	
	// choose which report to run
	protected val reportNumbers = parameters.getOrElse(Constants.Parameters.REPORT_NUMBER, Constants.Default.EMPTY_STRING).split("-")
	protected val logDir = parameters.getOrElse(Constants.Parameters.LOG_DIR, Constants.Default.FAIRY_LOG_DIR)
	protected var createDate = LocalDateTime.now(ZoneId.of("GMT+7"))
		.format(DateTimeFormatter.ofPattern(Constants.Default.DATETIME_FORMAT))
	
	final def run(mpDF: Map[String, DataFrame]): Unit = {
		
		if(validate() != true) {
			
			return
		}
		
		val result = execute(mpDF)
		write(result)
	}
	
	def validate(): Boolean = true
	def execute(mpDF: Map[String, DataFrame]): DataFrame
	def write(df: DataFrame): Unit

}
