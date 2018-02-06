package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.report.group._
import vng.ge.stats.report.util.{Constants, DateTimeUtils}

/**
  * Created by vinhdp on 1/10/17.
  */
class GroupDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var extraParam = ""
	
	
	override def readExtraParameter(): Unit = {
		
		extraParam = parameters.getOrElse("otherParams", "dafault_value")
	}
	
	/**
	  * Big problem here: which report need to run and how to cache?
	  */
	override def run(): Unit = {
		
		// using extra param here
		extraParam += "vinhdp"
		
		val prevDate = DateTimeUtils.getPreviousDate(logDate, timing)

		// loading activity data
		val activityDF = reader.loadFiles(gameCode, Files.ACTIVITY, logDate, timing, source, logDir = logDir)

		val newDF = reader.loadFiles(gameCode, Files.ACC_REGISTER, logDate, timing, source, logDir = logDir)
		
		val paymentDF = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, logDir = logDir)

		val firstChargeDF = reader.loadFiles(gameCode, Files.FIRST_CHARGE, logDate, timing, source, logDir = logDir)

		/** User report **/
		// calc active user
		new GroupActiveUserReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACTIVITY -> activityDF)
		)
		
		// calc account register
		new GroupAccountRegisterReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACC_REGISTER -> newDF)
		)
		
		// calc user retention report
		val prevActivityDF = reader.loadFile(gameCode, Files.ACTIVITY, prevDate, source, logDir = logDir)
		val curActivityDF = activityDF.where(s"log_date like '$logDate%'")
		
		new GroupUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.ACTIVITY -> prevActivityDF,
				Constants.LogTypes.ACTIVITY -> curActivityDF
			)
		)
		
		// calc new user retention
		val prevNewDF = reader.loadFile(gameCode, Files.ACC_REGISTER, prevDate, source, logDir = logDir)
		
		new GroupNewUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> prevNewDF,
				Constants.LogTypes.ACTIVITY -> curActivityDF
			)
		)
		
		// calc new user paying
		new GroupNewUserRevenueReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> newDF,
				Constants.LogTypes.PAYMENT -> paymentDF
			)
		)
		
		/** Payment report **/
		// calc revenue
		new GroupPaymentReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.PAYMENT -> paymentDF)
		)
		
		// calc first charge
		new GroupFirstChargeReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.FIRST_CHARGE -> firstChargeDF,
				Constants.LogTypes.PAYMENT -> paymentDF
			)
		)
		
		val prevFirstChargeDF = reader.loadFile(gameCode, Files.FIRST_CHARGE, prevDate, source, logDir = logDir)
		val curPaymentDF = paymentDF.where(s"log_date like '$logDate%'")
		
		// calc first charge retention
		new GroupFirstChargeRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.FIRST_CHARGE -> prevFirstChargeDF,
				Constants.LogTypes.PAYMENT -> curPaymentDF
			)
		)
	}
}