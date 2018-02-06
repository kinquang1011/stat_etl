package vng.ge.stats.report.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import vng.ge.stats.report.model.{KpiFormat, KpiGroupFormat}
import vng.ge.stats.report.util.{Constants, Logger}

/**
  * Created by vinhdp on 1/20/17.
  */
class CsvWriter (sparkSession: SparkSession) extends Writer(sparkSession) {
	
	format = Constants.DataSources.CSV
}
