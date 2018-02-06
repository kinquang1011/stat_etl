package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 15/08/2017.
  * release : 12/09/2017
  */
class CcnSea extends FairyFormatter("ccnsea") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logoutRaw: RDD[String] = null
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAME_LOG_DIR + "/ccnsea/[yyyy-MM-dd]/ccn_metric_login/ccn_metric_login-[yyyy-MM-dd]_*"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate, 1)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.GAME_LOG_DIR + "/ccnsea/[yyyy-MM-dd]/ccn_metric_logout/ccn_metric_logout-[yyyy-MM-dd]_*"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate, 1)
      logoutRaw = getRawLog(logoutPath)
    }

    val loginFilter = (s1: Array[String]) => {
      var rs = false
      if (s1(0).length != 0) {
        if ((MyUdf.timestampToDate(s1(0).toLong * 1000).startsWith(logDate)) && (s1.length >= 10)) {
          rs = true
        }
      }
      rs
    }
    val logoutFilter = (s1: Array[String]) => {
      var rs = false
      if (s1(0).length != 0) {
        if ((MyUdf.timestampToDate(s1(0).toLong * 1000).startsWith(logDate)) && (s1.length >= 17)) {
          rs = true
        }
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "0") {
        platform = "ios"
      } else if (s == "1") {
        platform = "android"
      }
      platform
    }
    val sf = Constants.FIELD_NAME
    val loginDS = loginRaw.map(line => line.split("\\t")) .filter(line => loginFilter(line)).map { r =>
      val dateTime = MyUdf.timestampToDate(r(0).toLong * 1000)
      val userId = r(1)
      val sid = r(2)
      val rid = r(3)
      val roleName = r(4)
      val platForm = getOs(r(5))
      val channel = r(9)
      val level = r(10)
      ("ccnsea", dateTime, userId, sid, rid, roleName, platForm, channel, level, 0, "login")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.CHANNEL, sf.LEVEL, sf.ONLINE_TIME, sf.ACTION)
    val logoutDS = loginRaw.map(line => line.split("\\t")).filter(line => logoutFilter(line)).map { r =>
      val dateTime = MyUdf.timestampToDate(r(0).toLong * 1000)
      val userId = r(1)
      val sid = r(2)
      val rid = r(3)
      val roleName = r(4)
      val platForm = getOs(r(6))
      val channel = r(9)
      val level = r(10)
      val total = r(16)
      ("ccnsea", dateTime, userId, sid, rid, roleName, platForm, channel, level, total, "logout")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.CHANNEL, sf.LEVEL, sf.ONLINE_TIME, sf.ACTION)
    loginDS.union(logoutDS)
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var registerRaw: RDD[String] = null
    val pattern = Constants.GAMELOG_DIR +"/ccnsea/[yyyy-MM-dd]/ccn_metric_register/ccn_metric_register-[yyyy-MM-dd]_*"
    val registerPath = PathUtils.generateLogPathDaily(pattern, logDate, 1)
    registerRaw = getRawLog(registerPath)
    val filter = (row: Array[String]) => {
      var rs = false
      if ((row.length >= 5) && (MyUdf.timestampToDate(row(0).toLong * 1000).startsWith(logDate))) {
        rs = true
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "0") {
        platform = "ios"
      } else if (s == "1") {
        platform = "android"
      }
      platform
    }
    val sf = Constants.FIELD_NAME
    val registerDs = registerRaw.map(line => line.split("\\t")).filter(line => filter(line)).map { r =>
      val dateTime = MyUdf.timestampToDate(r(0).toLong * 1000)
      val id = r(1)
      val sid = r(2)
      val os = getOs(r(4))
      ("ccnsea", dateTime, id, sid, os)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.OS)
    registerDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/ccnsea/[yyyy-MM-dd]/ccn_ifrs/ccn_ifrs_[yyyyMMdd]_*"
      val paymentPath = PathUtils.generateLogPathDaily(pattern, logDate, 1)
      paymentRaw = getRawLog(paymentPath)
    }

    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 15) {
        if (line(10)!=0 && line(11)!=0 &&  MyUdf.timestampToDate(line(3).toLong).startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split(",")).filter(line => paymentFilter(line)).map { line =>
      val dateTime = MyUdf.timestampToDate(line(3).toLong)
      val id = line(0)
      val sid = line(4)
      val gross_amt = line(10)
      val net_amt = line(11)
      ("ccnsea", dateTime, id, sid, gross_amt, net_amt)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }

}
