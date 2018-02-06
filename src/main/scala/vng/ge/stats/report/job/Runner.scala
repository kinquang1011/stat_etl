package vng.ge.stats.report.job

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import vng.ge.stats.report.loader._
import vng.ge.stats.report.util.{Constants, DateTimeUtils, Logger}

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

/**
  * Created by vinhdp on 1/9/17.
  */
object Runner {
	
	def main(args: Array[String]): Unit = {
		
		val parameters = this.getParameters(args)
		this.run(parameters)
	}
	
	def run(parameters: Map[String, String]): Unit = {
		
		val jobName = parameters.getOrElse(Constants.Parameters.JOB_NAME, Constants.Default.APP_NAME)
		val sparkSession = SparkSession.builder.master(Constants.Default.MASTER).appName(jobName)
			    .enableHiveSupport().getOrCreate()
		
		val gameCode = parameters(Constants.Parameters.GAME_CODE)
		val logDate = parameters(Constants.Parameters.LOG_DATE)
		val source = parameters.getOrElse(Constants.Parameters.SOURCE, Constants.Default.SOURCE)
		val runType =  parameters.getOrElse(Constants.Parameters.RUN_TYPE, Constants.Default.RUN_TYPE)
		val timings = parameters.getOrElse(Constants.Parameters.RUN_TIMING, Constants.Default.RUN_TIMING)
		val reportNumber = parameters.getOrElse(Constants.Parameters.REPORT_NUMBER, Constants.Default.REPORT_NUMBER)
		
		val groupId = parameters.getOrElse(Constants.Parameters.GROUP_ID, Constants.Default.EMPTY_STRING)
		
		var lstTiming = List[String]()
		timings.split(",").foreach { value => lstTiming = lstTiming ::: List(value) }
		var newParams: Map[String, String] = parameters
		val start = DateTime.now.getMillis
		
		Logger.info("\\_ Parameters: " + parameters)
		
		for(timing <- lstTiming) {
			
			newParams = newParams + (Constants.Parameters.GAME_CODE -> gameCode)
			newParams = newParams + (Constants.Parameters.LOG_DATE -> logDate)
			newParams = newParams + (Constants.Parameters.TIMING -> timing)
			newParams = newParams + (Constants.Parameters.SOURCE -> source)
			newParams = newParams + (Constants.Parameters.REPORT_NUMBER -> reportNumber)
			
			Logger.info("---------------------------------------------------------------------------------------------")
			Logger.info(s"\\_ Timing: $timing")
			Logger.info(s"\\_ ===============")
			
			breakable {
				
				// if run in rerun mode => only run on SUNDAY (timing = ac7), only run on end of month (timing = ac30, ac60)
				if(!Constants.Default.RUN_TYPE.equals(runType)){
					if((timing == Constants.Timing.AC30 || timing == Constants.Timing.AC60)
						&& !DateTimeUtils.isEndOfMonth(logDate)) {
						
						Logger.info("\\_ " + logDate + " is not end of month! Skip!", tab = 1)
						break
					}
					
					if(timing == Constants.Timing.AC7 && !DateTimeUtils.isEndOfWeek(logDate)){
						Logger.info("\\_ " + logDate + " is not Sunday! Skip!", tab = 1)
						break
					}
				}
				
				// choose appropriate loader for running report
				groupId match {
					case Constants.Default.EMPTY_STRING | Constants.Default.GAME => {
						new GameDataLoader(sparkSession, newParams).report()
					}
					case Constants.GroupId.SERVER => {
						new ServerDataLoader(sparkSession, newParams).report()
					}
					case Constants.GroupId.OS => {
						new OsDataLoader(sparkSession, newParams).report()
					}
					case Constants.GroupId.CHANNEL | Constants.GroupId.PACKAGE => {
						new GroupDataLoader(sparkSession, newParams).report()
					}
					case Constants.GroupId.REVENUE_BY_USER => {
						new RevenueByUserDataLoader(sparkSession, newParams).report()
					}
					case _ => {
						new GroupMappingDataLoader(sparkSession, newParams).report()
					}
				}
			}
		}
		
		val time = DateTime.now.getMillis - start
		Logger.info("\\_ Total execution time: " + time)
	}
	
	def getParameters(args: Array[String]): Map[String, String] = {
		
		var mapParameters: Map[String,String] = Map()
		for(x <- args){
			val xx = x.split("=")
			mapParameters += (xx(0) -> xx(1))
		}
		mapParameters
	}
}
