package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.report.group._
import vng.ge.stats.report.util.{Constants, DateTimeUtils}
import org.apache.spark.sql.functions.{broadcast, coalesce, col}

/**
  * Created by vinhdp on 1/10/17.
  */
class ServerDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var extraParam = ""
	protected var mrtgCCUCode = ""

	/** just for testing jdbc**/
	protected val jdbcUsername = "ubstats"
	protected val jdbcPassword = "pubstats"
	protected val jdbcHostname = "10.60.22.2"
	protected val jdbcPort = 3306
	protected val jdbcDatabase ="ubstats"
	protected val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
	protected val connectionProperties = new java.util.Properties()
	connectionProperties.put("driver", "com.mysql.jdbc.Driver")
	connectionProperties.put("user", "ubstats")
	connectionProperties.put("password", "pubstats")
	
	
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
		val mergeDF = reader.jdbc(jdbcUrl, "group_name_conf", Array(s"game_code = '$gameCode' and merge_id is not null"), connectionProperties)
			.select("id", "merge_id")
		
		// loading ccu data
		var ccuDF = reader.loadFile(gameCode, Files.CCU, logDate, source, logDir = logDir)
		//using mrtg ccu log, by canhtq
		if(!mrtgCCUCode.equalsIgnoreCase("0")){
			ccuDF = sparkSession.read.parquet(s"/ge/fairy/warehouse/flumeccu/" + logDate).where(s"cmdb_prd_code='$mrtgCCUCode'")
		}
		val mccuDF = ccuDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id"))
		
		// loading activity data
		val activityDF = reader.loadFiles(gameCode, Files.ACTIVITY, logDate, timing, source, logDir = logDir)
		val mactivityDF = activityDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		
		val newDF = reader.loadFiles(gameCode, Files.ACC_REGISTER, logDate, timing, source, logDir = logDir)
		val mnewDF = newDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		
		val paymentDF = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, logDir = logDir)
		val mpaymentDF = paymentDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		
		val firstChargeDF = reader.loadFiles(gameCode, Files.FIRST_CHARGE, logDate, timing, source, logDir = logDir)
		val mfirstChargeDF = firstChargeDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		
		/** Traffic report **/
		// calc ccu
		new GroupCcuReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.CCU -> mccuDF)
		)
		
		/** User report **/
		// calc active user
		new GroupActiveUserReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACTIVITY -> mactivityDF)
		)
		
		// calc account register
		new GroupAccountRegisterReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.ACC_REGISTER -> mnewDF)
		)
		
		// calc user retention report
		val prevActivityDF = reader.loadFile(gameCode, Files.ACTIVITY, prevDate, source, logDir = logDir)
		val mprevActivityDF = prevActivityDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		
		val mcurActivityDF = mactivityDF.where(s"log_date like '$logDate%'")
		
		new GroupUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.ACTIVITY -> mprevActivityDF,
				Constants.LogTypes.ACTIVITY -> mcurActivityDF
			)
		)
		
		// calc new user retention
		val prevNewDF = reader.loadFile(gameCode, Files.ACC_REGISTER, prevDate, source, logDir = logDir)
		val mprevNewDF = prevNewDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		
		new GroupNewUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> mprevNewDF,
				Constants.LogTypes.ACTIVITY -> mcurActivityDF
			)
		)
		
		// calc new user paying
		new GroupNewUserRevenueReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.ACC_REGISTER -> mnewDF,
				Constants.LogTypes.PAYMENT -> mpaymentDF
			)
		)
		
		/** Payment report **/
		// calc revenue
		new GroupPaymentReport(sparkSession, parameters).run(
			Map(Constants.LogTypes.PAYMENT -> mpaymentDF)
		)
		
		// calc first charge
		new GroupFirstChargeReport(sparkSession, parameters).run(
			Map(
				Constants.LogTypes.FIRST_CHARGE -> mfirstChargeDF,
				Constants.LogTypes.PAYMENT -> mpaymentDF
			)
		)
		
		val prevFirstChargeDF = reader.loadFile(gameCode, Files.FIRST_CHARGE, prevDate, source, logDir = logDir)
		val mprevFirstChargeDF = prevFirstChargeDF.as('o).join(mergeDF.as('m), col(s"o.sid") === col(s"m.id"), "left_outer")
			.withColumn("sid", coalesce(col("merge_id"), col("sid"))).drop(col(s"merge_id")).drop(col(s"m.id"))
		val mcurPaymentDF = mpaymentDF.where(s"log_date like '$logDate%'")
		
		// calc first charge retention
		new GroupFirstChargeRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.FIRST_CHARGE -> mprevFirstChargeDF,
				Constants.LogTypes.PAYMENT -> mcurPaymentDF
			)
		)
	}
}