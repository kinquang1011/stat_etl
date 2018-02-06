package vng.ge.stats.report.base

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.util.Constants

/**
  * Created by vinhdp on 1/9/17.
  */
abstract class DataPool(sparkSession: SparkSession, parameters: Map[String, String]) {
	
	protected val reader = new DataReader(sparkSession)
	
	// read all default parameter
	protected val gameCode = parameters(Constants.Parameters.GAME_CODE)
	protected val logDate = parameters(Constants.Parameters.LOG_DATE)
	protected val source = parameters.getOrElse(Constants.Parameters.SOURCE, Constants.Default.SOURCE)
	protected val timing = parameters.getOrElse(Constants.Parameters.TIMING, Constants.Default.TIMING)
	protected val calcId = parameters.getOrElse(Constants.Parameters.CALC_ID, Constants.Default.CALC_ID)
	protected val groupId = parameters.getOrElse(Constants.Parameters.GROUP_ID, Constants.Default.EMPTY_STRING)
	
	protected val reportNumber = parameters.getOrElse(Constants.Parameters.REPORT_NUMBER, Constants.Default.REPORT_NUMBER)
	protected val isHourly = parameters.getOrElse(Constants.Parameters.HOURLY_REPORT, Constants.Default.FALSE_STRING)
	protected val logDir = parameters.getOrElse(Constants.Parameters.LOG_DIR, Constants.Default.FAIRY_LOG_DIR)
	
	/** read file name from configuration types **/
	protected object Files {
		val CCU = parameters.getOrElse(Constants.LogTypes.CCU, Constants.LogNames.CCU)
		val ACTIVITY = parameters.getOrElse(Constants.LogTypes.ACTIVITY, Constants.LogNames.ACTIVITY)
		val ACC_REGISTER = parameters.getOrElse(Constants.LogTypes.ACC_REGISTER, Constants.LogNames.ACC_REGISTER)
		val PAYMENT = parameters.getOrElse(Constants.LogTypes.PAYMENT, Constants.LogNames.PAYMENT)
		val FIRST_CHARGE = parameters.getOrElse(Constants.LogTypes.FIRST_CHARGE, Constants.LogNames.FIRST_CHARGE)
		val TOTAL_LOGIN_ACCOUNT = parameters.getOrElse(Constants.LogTypes.TOTAL_LOGIN_ACC, Constants.LogNames.TOTAL_LOGIN_ACC)
		val TOTAL_PAID_ACCOUNT = parameters.getOrElse(Constants.LogTypes.TOTAL_PAID_ACC, Constants.LogNames.TOTAL_PAID_ACC)
	}
	
	protected def readExtraParameter(): Unit
	protected def run(): Unit
	
	/**
	  * Template pattern: every report must be run in the same workflow
	  */
	final def report(): Unit = {
		readExtraParameter()
		run()
	}
}
