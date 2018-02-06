package vng.ge.stats.report.report.adhoc

import vng.ge.stats.report.base.TReport

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.{DataPool, DataReader, DataWriter, TReport}
import vng.ge.stats.report.util.Constants

/**
  * Created by canhtq on 08/05/2017.
  */
class ManualSupport(sparkSession: SparkSession, parameters: Map[String, String]) extends TReport(sparkSession, parameters) {
  override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
    val spark = sparkSession
    import spark.implicits
    val markedDate="2017-03-31"
    val totalUserPath = ""
    val totalDs = spark.read.parquet(totalUserPath)

    sparkSession.emptyDataFrame
  }

  override def write(df: DataFrame): Unit = {

    // make savable List
  }
}
