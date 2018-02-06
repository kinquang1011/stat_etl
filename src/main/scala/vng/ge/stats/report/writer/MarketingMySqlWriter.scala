package vng.ge.stats.report.writer

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.model._
import vng.ge.stats.report.sql.mysql._
import vng.ge.stats.report.util.{Constants, Logger}

/**
  * Created by vinhdp on 1/9/17.
  */
class MarketingMySqlWriter(sparkSession: SparkSession) extends Writer(sparkSession) {
	
	format = Constants.DataSources.JDBC
	
	override def write(data: List[Any]) = {
		if(!data.isEmpty) {
			
			MarketingMysqlReport.insert(data.asInstanceOf[List[MarketingFormat]])
		} else {
			Logger.warning("\\_ Result is empty! Nothing to write!", tab = 2)
		}
	}
}
