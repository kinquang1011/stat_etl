package vng.ge.stats.etl.transform.adapter.base

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.udf.MyUdf.dateTimeIncrement
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by canhtq on 19/05/2017.
  */
class SdkFormatter(_gameCode:String)  extends Formatter(_gameCode,"sdk") {
  setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)


  import sqlContext.implicits._

  // chinh lai khai niem timezone 0: +7 :-1 +8; 1: +6
  var timezone: String = "0"
  var sdkSource: String = ""
  //Game code doc file
  var sdkGameCode: String = ""

  var changeRate: Double = 1



  private val makeOtherIfNull = udf { (str: String) => {

    if (str == null || str == "") "other" else str
  }
  }
  private val lowerCaseCol = udf { (os: String) => {
    if (os != null) {
      os.toLowerCase
    } else {
      os
    }
  }
  }

  private val logDateWithTimeZone = udf { (datetime: String, timezone: String) => {
    var gmt = 7
    gmt = gmt - timezone.toInt
    val time = dateTimeIncrement(datetime, gmt * 3600)
    time
  }
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    //todo ==> file path 2 ngay lientiep, xong dung func val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
    var dailyLogin: DataFrame = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/" + sdkSource + "/[yyyy-MM-dd]/" + sdkGameCode + "_Login_InfoLog/" + sdkGameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"

      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      dailyLogin = getJsonLog(logPath)
    } else {
      val logPattern = Constants.GAME_LOG_DIR + "/" + sdkSource + "/[yyyy-MM-dd]/" + sdkGameCode + "_Login_InfoLog/"
      val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
      dailyLogin = getJsonLog(logPath)

    }
    if (dailyLogin.rdd.isEmpty()) {
      dailyLogin = createEmptyActivityDs()
      dailyLogin
    } else {

      dailyLogin = dailyLogin.withColumn("rid", col("userID"))
      dailyLogin = dailyLogin.withColumn("os", makeOtherIfNull(col("os")))
      dailyLogin = dailyLogin.withColumn("device_os", lowerCaseCol(col("device_os")))
      dailyLogin = dailyLogin.withColumn("gameCode", lit(gameCode))
      dailyLogin = dailyLogin.withColumn("action", lit("login"))
      dailyLogin = dailyLogin.withColumn("updatetime", logDateWithTimeZone(col("updatetime"), lit(timezone)))
      dailyLogin = dailyLogin.filter($"updatetime".contains(logDate)).select("device", "userID", "type", "updatetime", "package_name", "device_id", "gameCode", "action", "os", "device_os", "rid")

      val sf = Constants.FIELD_NAME


      val loginDs = dailyLogin.toDF(sf.DEVICE, sf.ID, sf.CHANNEL, sf.LOG_DATE, sf.PACKAGE_NAME, sf.DID, sf.GAME_CODE, sf.ACTION, sf.OS_VERSION, sf.OS, sf.RID)
      loginDs
    }
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {

    val setAmtWithRate = udf { (amt: String, changeRate: Double) => {
      var vnd: Double = 0
      vnd = amt.toDouble * changeRate

      vnd.toString
    }
    }

    var payment: DataFrame = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/" + sdkSource + "/[yyyy-MM-dd]/Log_" + sdkGameCode + "_DBGAdd/" + "Log_" + sdkGameCode + "_DBGAdd-[yyyy-MM-dd].gz"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      payment = getJsonLog(logPath)


    } else {
      val logPattern = Constants.GAME_LOG_DIR + "/" + sdkSource + "/[yyyy-MM-dd]/Log_" + sdkGameCode + "_DBGAdd/"
      val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
      payment = getJsonLog(logPath)

    }
    if (payment.rdd.isEmpty()) {
      payment = createEmptyPaymentDs()
      payment
    } else {
      payment = payment.withColumn("rid", col("userID"))
      payment = payment.withColumn("gameCode", lit(gameCode))
      payment = payment.withColumn("pmcNetChargeAmt", setAmtWithRate(col("pmcNetChargeAmt"), lit(changeRate)))
      payment = payment.withColumn("pmcGrossChargeAmt", setAmtWithRate(col("pmcGrossChargeAmt"), lit(changeRate)))
      payment = payment.withColumn("updatetime", logDateWithTimeZone(col("updatetime"), lit(timezone)))
      payment = payment.filter($"updatetime".contains(logDate)).select("updatetime", "userID", "transactionID", "pmcID", "pmcNetChargeAmt", "pmcGrossChargeAmt", "gameCode", "rid")


      val sf = Constants.FIELD_NAME
      val paymentDs = payment.toDF(sf.LOG_DATE, sf.ID, sf.TRANS_ID, sf.PAY_CHANNEL, sf.NET_AMT, sf.GROSS_AMT, sf.GAME_CODE, sf.RID)


      paymentDs
    }
  }

}
