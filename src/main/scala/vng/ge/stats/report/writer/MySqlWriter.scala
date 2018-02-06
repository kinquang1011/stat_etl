package vng.ge.stats.report.writer

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.base.DataWriter
import vng.ge.stats.report.model.{KpiFormat, KpiGameRetentionFormat, KpiGroupFormat, Schemas}
import vng.ge.stats.report.sql.mysql.{MysqlGameReport, MysqlGameRetentionReport, MysqlGenericGroupReport, MysqlGroupReport}
import vng.ge.stats.report.util.{Constants, Logger}

/**
  * Created by vinhdp on 1/9/17.
  */
class MySqlWriter(sparkSession: SparkSession) extends Writer(sparkSession) {
	
	format = Constants.DataSources.JDBC
	
	override def write(data: List[Any]) = {
		if(!data.isEmpty) {
			
			val className = data.head.getClass.getSimpleName
			className match {
				case "KpiFormat" => writeGameReport(data.asInstanceOf[List[KpiFormat]])     // may be using writer.format.option.write
				case "KpiGroupFormat" => writeGroupReport(data.asInstanceOf[List[KpiGroupFormat]])
				case "KpiGameRetentionFormat" => writeGameRetentionReport(data.asInstanceOf[List[KpiGameRetentionFormat]])
				case _ => Logger.info("\\_ " + className + " not support yet!", tab = 2)
			}
		} else {
			Logger.warning("\\_ Result is empty! Nothing to write!", tab = 2)
		}
	}
	
	def writeGameReport(data: List[KpiFormat]): Unit = {
		Logger.info("\\_ Writing game data format", tab = 2)
		// using multiple writer depend on write option
		val calcId = extraOptions(Constants.Parameters.CALC_ID)
		MysqlGameReport.insert(data, calcId)
	}
	
	def writeGameRetentionReport(data: List[KpiGameRetentionFormat]): Unit = {
		Logger.info("\\_ Writing game data format", tab = 2)
		// using multiple writer depend on write option
		val calcId = extraOptions(Constants.Parameters.CALC_ID)
		MysqlGameRetentionReport.insert(data, calcId)
	}
	
	def writeGroupReport(data: List[KpiGroupFormat]): Unit = {
		Logger.info("\\_ Writing group data format", tab = 2)
		val calcId = extraOptions(Constants.Parameters.CALC_ID)
		val groupId = extraOptions(Constants.Parameters.GROUP_ID)
		
		/** Choose which table to store data, and how to process results **/
		groupId match {
			case Constants.GroupId.CHANNEL |
			     Constants.GroupId.PACKAGE => {
				
				MysqlGroupReport.insert(data, groupId, calcId)
                
                // insert into group_kpi_json
                MysqlGenericGroupReport.insertJson(data, groupId, calcId)
			}
			case Constants.GroupId.SERVER |
			     Constants.GroupId.OS |
			     Constants.GroupId.COUNTRY => {
				
				MysqlGroupReport.insertJson(data, groupId, calcId)
			}
			case _ => {
				Logger.info("\\_ Insert to generic group", tab = 2)
				MysqlGenericGroupReport.insertJson(data, groupId, calcId)
			}
		}
	}
}
