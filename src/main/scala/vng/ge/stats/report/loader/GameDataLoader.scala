package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.report.game._
import vng.ge.stats.report.util.{Constants, DateTimeUtils}

/**
  * Created by vinhdp on 1/10/17.
  */
class GameDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var extraParam = ""
	protected var mrtgCCUCode = ""
	override def readExtraParameter(): Unit = {
		
		extraParam = parameters.getOrElse("otherParams", "dafault_value")
		mrtgCCUCode = parameters.getOrElse("mrtg_ccu_code", "0")
	}
	
	/**
	  * Big problem here: which report need to run and how to cache?
	  */
	override def run(): Unit = {
		
		// using extra param here
		extraParam += "vinhdp"
		
		val prevDate = DateTimeUtils.getPreviousDate(logDate, timing)
		
		// loading ccu data
		var ccuDF = reader.loadFile(gameCode, Files.CCU, logDate, source, isHourly, logDir = logDir)
		//using mrtg ccu log, by canhtq
		if(!mrtgCCUCode.equalsIgnoreCase("0")){
			ccuDF = sparkSession.read.parquet(s"/ge/fairy/warehouse/flumeccu/" + logDate).where(s"cmdb_prd_code='$mrtgCCUCode'")
		}
		// loading activity data
		var df = reader.loadFiles(gameCode, Files.ACTIVITY, logDate, timing, source, isHourly, logDir = logDir)
		val activityDF = df
		
		val newDF = reader.loadFiles(gameCode, Files.ACC_REGISTER, logDate, timing, source, isHourly, logDir = logDir)
		
		df = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, isHourly, logDir = logDir)
		val paymentDF = df
		
		val firstChargeDF = reader.loadFiles(gameCode, Files.FIRST_CHARGE, logDate, timing, source, isHourly, logDir = logDir)
		val totalLoginAccDF = reader.loadFile(gameCode, Files.TOTAL_LOGIN_ACCOUNT, logDate, source, isHourly, logDir = logDir)
		
		// load by timing is used for active user, register user, payment, first charge
		/** Game retention **/
		new GameRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACTIVITY -> activityDF,
				Constants.LogTypes.TOTAL_LOGIN_ACC -> totalLoginAccDF
			)
		)
		
		/** Traffic report **/
		// calc ccu
		new CcuReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.CCU -> ccuDF)
		)
		
		/** User report **/
		// calc active user
		new ActiveUserReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACTIVITY -> activityDF)
		)
		
		// calc playing time
		new PlayingTimeReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACTIVITY -> activityDF)
		)
		
		// calc account register
		new AccountRegisterReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACC_REGISTER -> newDF)
		)
		
		// calc user retention report
		val prevActivityDF = reader.loadFile(gameCode, Files.ACTIVITY, prevDate, source, isHourly, logDir = logDir).select(s"$calcId").distinct
		val curActivityDF = activityDF.where(s"log_date like '$logDate%'").select(s"$calcId").distinct
		
		new UserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.ACTIVITY -> prevActivityDF,
				Constants.LogTypes.ACTIVITY -> curActivityDF
			)
		)
		
		// calc new user retention
		val prevNewDF = reader.loadFile(gameCode, Files.ACC_REGISTER, prevDate, source, isHourly, logDir = logDir)
		
		new NewUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> prevNewDF,
				Constants.LogTypes.ACTIVITY -> curActivityDF
			)
		)
		
		// calc new user paying
		new NewUserRevenueReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> newDF,
				Constants.LogTypes.PAYMENT -> paymentDF
			)
		)
		
		/** Payment report **/
		// calc revenue
		new PaymentReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.PAYMENT -> paymentDF)
		)
		
		// calc first charge
		new FirstChargeReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.FIRST_CHARGE -> firstChargeDF,
				Constants.LogTypes.PAYMENT -> paymentDF
			)
		)
		
		val prevFirstChargeDF = reader.loadFile(gameCode, Files.FIRST_CHARGE, prevDate, source, isHourly, logDir = logDir)
		val curPaymentDF = paymentDF.where(s"log_date like '$logDate%'")
		
		// calc first charge retention
		new FirstChargeRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.FIRST_CHARGE -> prevFirstChargeDF,
				Constants.LogTypes.PAYMENT -> curPaymentDF
			)
		)
	}
}