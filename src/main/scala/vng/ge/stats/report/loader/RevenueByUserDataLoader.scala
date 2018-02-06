package vng.ge.stats.report.loader

import org.apache.spark.sql.SparkSession
import vng.ge.stats.report.base.DataPool
import vng.ge.stats.report.model.Schemas
import vng.ge.stats.report.report.adhoc.RevenueByUser
import vng.ge.stats.report.report.group._
import vng.ge.stats.report.util.Constants.LogNames
import vng.ge.stats.report.util.{Constants, DateTimeUtils}

/**
  * Created by canhtq on 1/10/17.
  */
class RevenueByUserDataLoader(sparkSession: SparkSession, parameters: Map[String, String])
	extends DataPool(sparkSession, parameters) {
	
	protected var extraParam = ""
	
	
	override def readExtraParameter(): Unit = {
		
		extraParam = parameters.getOrElse("otherParams", "dafault_value")
	}
	
	/**
	  * Big problem here: which report need to run and how to cache?
		* timing=a1,a7,a3
	  */
	override def run(): Unit = {
		
		// using extra param here
		extraParam += "canhtq"
		val prevDate = DateTimeUtils.getPreviousDate(logDate, timing)
		val paymentDF = reader.loadFile(gameCode, Files.PAYMENT, logDate, source, logDir = logDir)
		val newRegDF = reader.loadFile(gameCode, Files.ACC_REGISTER, logDate, source, logDir = logDir)
		val revenueByUserDF = reader.loadFile(gameCode, LogNames.REVENUE_BY_USER, prevDate, source,  logDir = logDir, defaultSchema = Schemas.RevenueByUser)
		/** User report **/
		// calc active user
		new RevenueByUser(sparkSession, parameters).run(
			Map(Constants.LogTypes.PAYMENT -> paymentDF,Constants.LogTypes.ACC_REGISTER -> newRegDF,Constants.LogTypes.REVENUE_BY_USER -> revenueByUserDF)
		)

	}
}