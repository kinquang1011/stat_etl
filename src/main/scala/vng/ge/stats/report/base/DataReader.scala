package vng.ge.stats.report.base

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import vng.ge.stats.report.model.Schemas
import vng.ge.stats.report.util.{Constants, Logger, PathUtils}

import scala.util.{Failure, Success, Try}
import util.control.Breaks._

/**
  * Created by vinhdp on 1/9/17.
  */
class DataReader(sparkSession: SparkSession) {
	
	var source: String = Constants.DataSources.PARQUET
	var userSpecifiedSchema: Option[StructType] = None
	var extraOptions = new scala.collection.mutable.HashMap[String, String]
	
	def format(source: String): DataReader = {
		this.source = source
		this
	}
	
	def option(key: String, value: String): DataReader = {
		this.extraOptions += (key -> value)
		this
	}
	
	def parquet(paths: String*): DataFrame = {
		sparkSession.read.options(extraOptions).parquet(paths:_*)
	}
	
	def parquet(path: String): DataFrame = {
		sparkSession.read.options(extraOptions).parquet(path)
	}
	
	def csv(paths: String*): DataFrame = {
		sparkSession.read.options(extraOptions).csv(paths:_*)
	}
	
	def csv(path: String): DataFrame = {
		sparkSession.read.options(extraOptions).csv(path)
	}
	
	def jdbc(url: String, table: String, properties: Properties): DataFrame = {
		sparkSession.read.options(extraOptions).jdbc(url, table, properties)
	}
	
	def jdbc(url: String, table: String, predicates: Array[String], properties: Properties): DataFrame = {
		sparkSession.read.options(extraOptions).jdbc(url, table, predicates, properties)
	}
	
	def hCatalog(): Unit = {
		
	}
	
	/**
	  * Load multiple files using "path" at "logDate" by "timing"
	  *
	  * @param path
	  * @param logDate
	  * @param timing
	  * @return
	  */
	final def load(path: String, logDate: String, timing: String): DataFrame = {
		
		var lstFiles = List[String]()
		val df = source match {
			case Constants.DataSources.PARQUET => {
				lstFiles = PathUtils.getFiles(path, logDate, timing)
				this.parquet(lstFiles:_*)
			}
			case Constants.DataSources.CSV => {
				lstFiles = PathUtils.getFiles(path, logDate, timing)
				this.csv(lstFiles:_*)
			}
			case Constants.DataSources.HCATALOG => {
				
				Logger.warning(source + " is not supported yet!")
				sparkSession.emptyDataFrame
			}
			case Constants.DataSources.JDBC => {
				
				Logger.warning(source + " is not supported yet!")
				sparkSession.emptyDataFrame
			}
			case _ => {
				Logger.warning(source + " can not be resolved!")
				sparkSession.emptyDataFrame
			}
		}
		
		df
	}
	
	/**
	  * Load one file at "path"
	  *
	  * @param path
	  * @return
	  */
	final def load(path: String): DataFrame = {
		
		val df = source match {
			case Constants.DataSources.PARQUET => {
				this.parquet(path)
			}
			case Constants.DataSources.CSV => {
				this.csv(path)
			}
			case Constants.DataSources.HCATALOG => {
				Logger.warning(source + " is not supported yet!")
				null
			}
			case Constants.DataSources.JDBC => {
				
				Logger.warning(source + " is not supported yet!")
				null
			}
			case _ => {
				Logger.warning(source + " can not be resolved!")
				null
			}
		}
		
		df
	}
	
	/**
	  * Load file after building path using element
	  *
	  * @param elements
	  * @return
	  */
	final def load(elements: String*): DataFrame = {
		
		val path = PathUtils.makePath(elements:_*)
		this.load(path)
	}
	
	/**
	  * Load one file at logDate
	  *
	  * @param gameCode game code
	  * @param fileName file name in HDFS
	  * @param logDate date in format yyyy-MM-dd
	  * @return
	  */
	final def loadFile(gameCode: String, fileName: String, logDate: String, source: String,
	                   isHourly: String = Constants.Default.FALSE_STRING,
	                   logDir: String = Constants.Default.FAIRY_LOG_DIR,
	                   defaultSchema: StructType = Schemas.Unknow): DataFrame = {
		
		val filePath = PathUtils.makeFilePath(gameCode, fileName, isSDK(source), isHourly, logDir)
		val file = PathUtils.makePath(filePath, logDate)
		Logger.info("\\_ Reading " + file, tab = 1)
		/*if(this.isEmpty(file)){
		
			Logger.warning(s"$file is not found => using default schema!")
			val schema = Schemas.getSchema(fileName)
			return sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
		}*/
		
		val result = Try(this.load(filePath, logDate))
		result match {
			case Success(v) => {
				return v
			}
			case Failure(e) => {
				
				Logger.warning(s"\\_ Loading file failed: " + e.getMessage, tab = 2)
				Logger.warning(s"\\_ $file is not found => using default schema!", tab = 2)
				val schema = Schemas.getSchema(fileName, defaultSchema)
				return sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
			}
		}
	}
	
	/**
	  * Load multiple files before logDate by timing inclusive
	  *
	  * @param gameCode game code
	  * @param fileName file name in HDFS
	  * @param logDate date in format yyyy-MM-dd
	  * @param timing may be a1, a3, a7, a14, a30, a60, a90, ac7, ac30
	  * @return
	  */
	final def loadFiles(gameCode: String, fileName: String, logDate: String,
	                    timing: String, source: String, isHourly: String = Constants.Default.FALSE_STRING,
	                    logDir: String = Constants.Default.FAIRY_LOG_DIR,
	                    defaultSchema: StructType = Schemas.Unknow): DataFrame = {
		
		val filePath = PathUtils.makeFilePath(gameCode, fileName, isSDK(source), isHourly, logDir)
		val filePattern = PathUtils.getFilePatterns(filePath, logDate, timing)
		Logger.info("\\_ Reading " + filePath, tab = 1)
		
		val results = Try(this.load(filePattern))
		
		results match {
			case Failure(e) => {
				Logger.warning(s"\\_ Loading file failed: " + e.getMessage, tab = 2)
				Logger.warning(s"\\_ $filePattern is not found => using default schema!", tab = 2)
				val schema = Schemas.getSchema(fileName, defaultSchema)
				return sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
			}
			case Success(v) => {
				return v
			}
		}
	}
	
	def isSDK(source: String): Boolean = {
		
		if(source == Constants.IN_GAME || source == Constants.PAYMENT) {
			return false
		}
		
		true
	}
	
	def isEmpty(file: String): Boolean = {
		
		val fs = FileSystem.get(new Configuration(true));
		var isEmpty = true;
		val path = new Path(file)
		if (fs.exists(path)) {
			
			isEmpty = false
		}
		
		isEmpty
	}
}
