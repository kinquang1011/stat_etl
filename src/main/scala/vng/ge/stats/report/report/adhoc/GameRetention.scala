package vng.ge.stats.report.report.adhoc

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{DataPool, DataReader, DataWriter, TReport}
import vng.ge.stats.report.util.Constants

/**
  * Created by vinhdp on 2/9/17.
  */
class GameRetention(sparkSession: SparkSession, parameters: Map[String, String])
	extends TReport(sparkSession, parameters) {
	
	override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
		// load params
		
		// read file
		val requiredDF = reader.loadFiles(gameCode, Constants.LogTypes.ACTIVITY, logDate, timing, source)
		
		// execute transform & action
		
		// generate results in DF
		sparkSession.emptyDataFrame
		
		// or write result here
		writer.format(Constants.DataSources.JDBC).option("name", "value").write(Nil)
		sparkSession.emptyDataFrame
	}
	
	override def write(df: DataFrame): Unit = {
		
		// make savable List
	}
}
