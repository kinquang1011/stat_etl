package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.utils.Common


/**
  * Created by quangctn on 12/09/2017.
  * release :
  * Contact point : tuanntt
  * cmdb_prd_code=137
  */
class Dt2 extends FairyFormatter("dt2") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logoutRaw: RDD[String] = null
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAME_LOG_DIR + "/zombie3d2m/[yyyyMMdd]/loggame/zombie3d2m_gamelog/login-[yyyy-MM-dd].gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate, 2)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.GAME_LOG_DIR + "/zombie3d2m/[yyyyMMdd]/loggame/zombie3d2m_gamelog/logout-[yyyy-MM-dd].gz"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate, 2)
      logoutRaw = getRawLog(logoutPath)
    }

    val loginFilter = (r: Array[String]) => {
      var rs = false
      if (r.length >= 17) {
        val newDate = r(2).replace("/", "-")
        if (newDate.startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }
    val logoutFilter = (r: Array[String]) => {
      var rs = false
      if (r.length >= 5) {
        val newDate = r(2).replace("/", "-")
        if (newDate.startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "1") {
        platform = "ios"
      } else if (s == "2") {
        platform = "android"
      }
      platform
    }

    val getChannel = (id: String) => {
      var channel = ""
      id match {
        case "0" => {
          channel = "Unknown"
        }
        case "1" => {
          channel = "Guest"
        }
        case "4" => {
          channel = "Facebook"
        }
        case "5" => {
          channel = "Google"
        }
        case _ =>
      }
      channel
    }
    val sf = Constants.FIELD_NAME
    val loginDS = loginRaw.map(line => line.split("\\t")).filter(line => loginFilter(line)).map { r =>
      val dateTime = r(2).replace("/", "-")
      val sid = r(1)
      val userId = r(3)
      val channel = getChannel(r(4))
      val os = getOs(r(6))
      val resolution = r(9) + "," + r(10)
      val ip = r(16)
      val country = r(r.length - 1)
      (dateTime, sid, userId, channel, os, resolution, ip, "0", "login", country)
    }.toDF(sf.LOG_DATE, sf.SID, sf.ID, sf.CHANNEL, sf.OS, sf.RESOLUTION, sf.IP, sf.ONLINE_TIME, sf.ACTION, sf.COUNTRY_CODE)

    val logoutDS = logoutRaw.map(line => line.split("\\t")).filter(line => logoutFilter(line)).map { r =>
      val sid = r(1)
      val dateTime = r(2).replace("/", "-")
      val userId = r(3)
      val onlineTime = r(5)
      (dateTime, sid, userId, "", "", "", "", onlineTime, "logout", "")
    }.toDF(sf.LOG_DATE, sf.SID, sf.ID, sf.CHANNEL, sf.OS, sf.RESOLUTION, sf.IP, sf.ONLINE_TIME, sf.ACTION, sf.COUNTRY_CODE)
    loginDS.union(logoutDS).withColumn(sf.GAME_CODE, lit("dt2"))
  }

  /*override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var registerRaw: RDD[String] = null
    val pattern = Constants.GAMELOG_DIR + "/zombie3d2m/[yyyyMMdd]//loggame/zombie3d2m_gamelog/register-[yyyy-MM-dd].gz"
    val registerPath = PathUtils.generateLogPathDaily(pattern, logDate, 2)
    registerRaw = getRawLog(registerPath)
    val filter = (row: Array[String]) => {
      var rs = false
      if (row.length >= 6) {
        val newDate = row(1).replace("/", "-")
        if (newDate.startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val registerDs = registerRaw.map(line => line.split("\\t")).filter(line => filter(line)).map { r =>
      val sid = r(0)
      val dateTime = r(1).replace("/", "-")
      val userId = r(2)
      ("dt2", dateTime, userId, sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID)
    registerDs
  }*/

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      if ((logDate.equalsIgnoreCase("2017-12-02")
        || (logDate.equalsIgnoreCase("2017-12-03"))
        || (logDate.equalsIgnoreCase("2017-12-04"))
        || (logDate.equalsIgnoreCase("2017-12-05"))
        )) {

        val paymentPath = "/ge/gamelogs/zombie3d2m/20171205/logifrs/*"
        paymentRaw = getRawLog(Array(paymentPath))
      } else {
        val pattern = Constants.GAMELOG_DIR + "/zombie3d2m/[yyyyMMdd]/logifrs/payment-[yyyy-MM-dd]_*"
        val paymentPath = PathUtils.generateLogPathDaily(pattern, logDate, 1)
        paymentRaw = getRawLog(paymentPath)
      }
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "1") {
        platform = "ios"
      } else if (s == "2") {
        platform = "android"
      }
      platform
    }

    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 16) {
        val newDate = line(2).replace("/", "-")

        //Log date && status : 1 success
        if (newDate.startsWith(logDate) && line(11).equalsIgnoreCase("1")) {
          rs = true
        }
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\t")).filter(line => paymentFilter(line)).map { r =>
      val sid = r(1)
      val time = r(2).replace("/", "-")
      val userId = r(3)
      val os = getOs(r(4))
      val transId = r(5)
      val ip = r(6)
      val money = r(14)

      ("dt2", time, sid, userId, os, transId, ip, money, money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ID, sf.OS, sf.TRANS_ID, sf.IP, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }

  override def getCountryDf(logDate: String, activeDf: DataFrame): DataFrame = {
    val field = Constants.FIELD_NAME
    var countryDf = getActivityDs(logDate, "").select(field.GAME_CODE, field.LOG_DATE, field.ID, field.COUNTRY_CODE)
    countryDf = countryDf.withColumn(field.LOG_DATE, substring(col(field.LOG_DATE), 0, 10))
      .withColumn(field.LOG_TYPE, lit("login"))
    countryDf


  }

}
