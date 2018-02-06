package vng.ge.stats.report.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import vng.ge.stats.report.model.{KpiFormat, KpiGroupFormat}
import vng.ge.stats.report.util.{Constants, Logger}

/**
  * Created by vinhdp on 1/20/17.
  */
abstract class Writer(sparkSession: SparkSession) {
	
	protected var format = Constants.Default.EMPTY_STRING
	protected var mode: String = "error"
	protected var path: String = ""
	
	protected var extraOptions = new scala.collection.mutable.HashMap[String, String]
	
	def option(key: String, value: String): Writer = {
		this.extraOptions += (key -> value)
		this
	}
	
	def options(options: scala.collection.Map[String, String]): Writer = {
		this.extraOptions ++= options
		this
	}
	
	def mode(mode: String): Writer = {
		this.mode = mode
		this
	}
	
	def path(path: String): Writer = {
		this.path = path
		this
	}
	
	def write(df: DataFrame): Unit = {
		
		if ("" == path) {
			Logger.error("Please, provide destination for file output!")
			throw new Exception("Missing file output location")
		} else {
			
			if(path == "/user/fairy/vinhdp/csv-result") {
				Logger.info(s"Writing $format in path: $path")
				df.write.format(format).options(extraOptions).mode(mode).save(path)
			}
		}
	}
	
	def write(data: List[Any]): Unit = {
		
		import sparkSession.implicits._
		if (!data.isEmpty) {
			
			var df: DataFrame = sparkSession.emptyDataFrame
			val className = data.head.getClass.getSimpleName
			className match {
				case "KpiFormat" => {
					
					df = data.asInstanceOf[List[KpiFormat]].toDF
				}
				case "KpiGroupFormat" => {
					
					df = data.asInstanceOf[List[KpiGroupFormat]].toDF
				}
				case _ => Logger.info(className + " not support yet! Prepare to write empty DataFrame!")
			}
			
			this.write(df)
			
		} else {
			Logger.warning("Result is empty! Nothing to write!")
		}
	}
}
