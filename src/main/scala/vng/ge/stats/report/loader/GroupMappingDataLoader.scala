package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.model.Schemas
import vng.ge.stats.report.report.game._
import vng.ge.stats.report.report.group._
import vng.ge.stats.report.util.{Constants, DateTimeUtils}

/**
  * Created by vinhdp on 2/15/17
  * Mapping file schema must be: id: String, groupId: String
  */
class GroupMappingDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var MAPPING_FILE_NAME = ""
	protected var MAPPING_FIELD = "group"
	
	override def readExtraParameter(): Unit = {
		
		MAPPING_FILE_NAME = parameters(Constants.LogTypes.GROUP_MAPPING_FILE)
		MAPPING_FIELD = parameters.getOrElse("mapping_field", MAPPING_FIELD)
	}
	
	/** Big problem here: which report need to run and how to cache? **/
	override def run(): Unit = {
		
		val prevDate = DateTimeUtils.getPreviousDate(logDate, timing)
		
		// loading mapping file
		/** how to process file that contain multiple calcid => groupid, which groupid need to be remove?**/
		val mappingDF = reader.loadFiles(gameCode, MAPPING_FILE_NAME, logDate, timing, source, logDir = logDir)
			.orderBy(asc(calcId), desc("log_date")).dropDuplicates(Seq(calcId))
			.select(s"$calcId", s"$MAPPING_FIELD")
			.withColumnRenamed(s"$MAPPING_FIELD", s"$groupId")   // define which data needed
		
		val prevMappingDF = reader.loadFile(gameCode, MAPPING_FILE_NAME, prevDate, source, logDir = logDir, defaultSchema = Schemas.Mapping)
			.orderBy(asc(calcId), desc("log_date")).dropDuplicates(Seq(calcId))
			.select(s"$calcId", s"$MAPPING_FIELD")
			.withColumnRenamed(s"$MAPPING_FIELD", s"$groupId")   // define which data needed
		
		// loading activity data
		val activityDF = reader.loadFiles(gameCode, Files.ACTIVITY, logDate, timing, source, logDir = logDir)
		// map activity => get groupId
		val mactivityDF = activityDF.as('o).join(mappingDF.as('m),
			col(s"o.$calcId") === col(s"m.$calcId"), "left_outer").drop(col(s"m.$calcId"))
		
		val newDF = reader.loadFiles(gameCode, Files.ACC_REGISTER, logDate, timing, source, logDir = logDir)
		// map newDF => get groupId
		val mnewDF = newDF.as('o).join(mappingDF.as('m),
			col(s"o.$calcId") === col(s"m.$calcId"), "left_outer").drop(col(s"m.$calcId"))

		val paymentDF = reader.loadFiles(gameCode, Files.PAYMENT, logDate, timing, source, logDir = logDir)
		// map paymentDF => get groupId
		val mpaymentDF = paymentDF.as('o).join(mappingDF.as('m),
			col(s"o.$calcId") === col(s"m.$calcId"), "left_outer").drop(col(s"m.$calcId"))   // drop mapping side calcId column
		
		val firstChargeDF = reader.loadFiles(gameCode, Files.FIRST_CHARGE, logDate, timing, source, logDir = logDir)
		// map firstchargeDF => get groupId
		val mfirstChargeDF = firstChargeDF.as('o).join(mappingDF.as('m),
			col(s"o.$calcId") === col(s"m.$calcId"), "left_outer").drop(col(s"m.$calcId"))
		
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
		// map prevActivityDF = get groupId
		val mprevActivityDF = prevActivityDF.as('o).join(prevMappingDF.as('pm),
			col(s"o.$calcId") === col(s"pm.$calcId"), "left_outer").drop(col(s"pm.$calcId"))
		
		val mcurActivityDF = mactivityDF.where(s"log_date like '$logDate%'")  // filter from cache
		
		new GroupUserRetentionReport(sparkSession, parameters).run(
			Map(
				Constants.PREV + Constants.LogTypes.ACTIVITY -> mprevActivityDF,
				Constants.LogTypes.ACTIVITY -> mcurActivityDF
			)
		)
		
		// calc new user retention
		val prevNewDF = reader.loadFile(gameCode, Files.ACC_REGISTER, prevDate, source, logDir = logDir)
		// map prev newDF => get groupId
		val mprevNewDF = prevNewDF.as('o).join(prevMappingDF.as('pm),
			col(s"o.$calcId") === col(s"pm.$calcId"), "left_outer").drop(col(s"pm.$calcId"))
		
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
		// map mprevFirstChargeDF => get groupId
		val mprevFirstChargeDF = prevFirstChargeDF.as('o).join(prevMappingDF.as('pm),
			col(s"o.$calcId") === col(s"pm.$calcId"), "left_outer").drop(col(s"pm.$calcId"))
		
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
