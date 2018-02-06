package vng.ge.stats.report.base

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import vng.ge.stats.report.util.{Constants, Logger}
import vng.ge.stats.report.writer.{CsvWriter, MySqlWriter, ParquetWriter, Writer}

/**
  * Created by vinhdp on 1/9/17.
  */
class DataWriter(sparkSession: SparkSession, parameters: Map[String, String]) {
	
	var format: String = Constants.DataSources.JDBC
	var mode: String = "error"
	var path: String = ""
	
	var userSpecifiedSchema: Option[StructType] = None
	var extraOptions = new scala.collection.mutable.HashMap[String, String]
	
	def format(format: String): DataWriter = {
		this.format = format
		this
	}
	
	def mode(mode: String): DataWriter = {
		this.mode = mode
		this
	}
	
	def path(path: String): DataWriter = {
		this.path = path
		this
	}
	
	def option(key: String, value: String): DataWriter = {
		this.extraOptions += (key -> value)
		this
	}
	
	def writeParquet(df: DataFrame, path: String): Unit = {
		
		if ("" == path) {
			Logger.error("Please, provide destination for file output!")
			throw new Exception("Missing file output location")
		} else {
            
			df.coalesce(1).write.format(Constants.DataSources.PARQUET).options(extraOptions).mode(mode).save(path)
		}
	}
	
	def write(data: List[Any]): Unit = {
		
		var writer: Writer = null
		format match {
			case Constants.DataSources.PARQUET => {
				writer = new ParquetWriter(sparkSession)
			}
			case Constants.DataSources.CSV => {
				writer = new CsvWriter(sparkSession)
			}
			case Constants.DataSources.HCATALOG => {
				
				Logger.warning(format + " is not supported yet!")
				return
			}
			case Constants.DataSources.JDBC => {
				
				val calcId = parameters.getOrElse(Constants.Parameters.CALC_ID, Constants.Default.CALC_ID)
				val groupId = parameters.getOrElse(Constants.Parameters.GROUP_ID, Constants.Default.EMPTY_STRING)
				
				this.option("calc_id", calcId).option("group_id", groupId)
				writer = new MySqlWriter(sparkSession)
			}
			case _ => {
				Logger.warning(format + " can not be resolved!")
				return
			}
		}
		
		/** Be careful when change to write all report using JDBC format because some model still not implement to convert to jdbc in MysqlWriter
		  * CSV & Parquet storage is safe if model class is added in Writer base class
		  */
		writer.options(extraOptions).mode(mode).path(path).write(data)
	}
}
