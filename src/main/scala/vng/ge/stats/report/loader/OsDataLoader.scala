package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.report.group._
import vng.ge.stats.report.util.{Constants, DateTimeUtils}
import org.apache.spark.sql.functions._

/**
  * Created by vinhdp on 2/15/17.
  */
class OsDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var extraParam = ""
	
	private val extractOs = udf {(os: String) =>
		
		var osLower = ""
		if(os != null ) osLower = os.toLowerCase()
		
		osLower match {
			case "android"    => "android"
			case "ios"        => "ios"
			case "iphone os"  => "ios"
			case "iphone"     => "ios"
			case _            => ""
		}
	}
	
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
		
		// loading ccu data
		val ccuDF = reader.loadFile(gameCode, Files.CCU, logDate, source, logDir = logDir)
		
		// loading activity data
		var df = reader.loadFiles(gameCode, Files.ACTIVITY, logDate, timing, source, logDir = logDir)
		val activityDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		//val activityDF = df.withColumn(groupId, extractOs(df(groupId)))
		
		df = reader.loadFiles(gameCode, Files.ACC_REGISTER, logDate, timing, source, logDir = logDir)
		//val newDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		val newDF = df.withColumn(groupId, extractOs(df(groupId)))
		
		df = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, logDir = logDir)
		val paymentDF = df.withColumn(groupId, extractOs(df(groupId)))
		
		df = reader.loadFiles(gameCode, Files.FIRST_CHARGE, logDate, timing, source, logDir = logDir)
		val firstChargeDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		//val firstChargeDF = df.withColumn(groupId, extractOs(df(groupId)))
		
		/** Traffic report **/
		// calc ccu
		/*new GroupCcuReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.CCU -> ccuDF)
		)*/
		
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
		df = reader.loadFile(gameCode, Files.ACTIVITY, prevDate, source, logDir = logDir)
		val prevActivityDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		//val prevActivityDF = df.withColumn(groupId, extractOs(df(groupId)))
		//val curActivityDF = activityDF.where(s"log_date like '$logDate%'")
		df = reader.loadFile(gameCode, Files.ACTIVITY, logDate, source, logDir = logDir)
		val curActivityDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		
		new GroupUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.ACTIVITY -> prevActivityDF,
				Constants.LogTypes.ACTIVITY -> curActivityDF
			)
		)
		
		// calc new user retention
		df = reader.loadFile(gameCode, Files.ACC_REGISTER, prevDate, source, logDir = logDir)
		//val prevNewDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		val prevNewDF = df.withColumn(groupId, extractOs(df(groupId)))
			
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
		
		df = reader.loadFile(gameCode, Files.FIRST_CHARGE, prevDate, source, logDir = logDir)
		val prevFirstChargeDF = df.withColumn(groupId, extractOs(df(groupId))).orderBy(desc(calcId), desc(groupId)).dropDuplicates(Seq(calcId))
		//val prevFirstChargeDF = df.withColumn(groupId, extractOs(df(groupId)))
		//val curPaymentDF = paymentDF.where(s"log_date like '$logDate%'")
		val curPaymentDF = reader.loadFile(gameCode, Files.PAYMENT, logDate, source, logDir = logDir)
		
		// calc first charge retention
		new GroupFirstChargeRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.FIRST_CHARGE -> prevFirstChargeDF,
				Constants.LogTypes.PAYMENT -> curPaymentDF
			)
		)
	}
}