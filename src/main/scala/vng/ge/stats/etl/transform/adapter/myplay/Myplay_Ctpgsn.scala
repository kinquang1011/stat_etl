package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 08/02/2017.
  */
class Myplay_Ctpgsn extends MyplayFormatter("mp_cotyphu") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_cotyphu-*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath, logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    val filterLog = (line: String) => {
      var rs = false
      if (line.startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val gross_amt = line(4)
      val id = line(2)
      val net_amt = gross_amt.toDouble * line(9).toDouble
      ("mp_cotyphu", dateTime, id, gross_amt, net_amt)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.WAREHOUSE_DIR + "/ctpgsn/login/[yyyy-MM-dd]/*.gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.WAREHOUSE_DIR + "/ctpgsn/logout/[yyyy-MM-dd]/*.gz"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
      logoutRaw = getRawLog(logoutPath)
    } else {
      //hourly
    }
    val loginFilter = (arr: Array[String]) => {
      var rs = false
      if (arr.length == 17 && arr(0).startsWith(logDate) && arr(16) == "1") {
        rs = true
      }
      rs
    }

    val logoutFilter = (arr: Array[String]) => {
      var rs = false
      if (arr.length == 19 && arr(0).startsWith(logDate) && arr(17) == "1") {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => loginFilter(line)).map { arr =>
      ("mp_cotyphu", arr(0), "logout", arr(4), arr(2), arr(10), arr(8), arr(6), arr(7), arr(16))
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.LEVEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.ONLINE_TIME)
    val logoutDs = logoutRaw.map(line => line.split("\\t")).filter(line => logoutFilter(line)).map { arr =>
      ("mp_cotyphu", arr(0), "logout", arr(4), arr(2), arr(10), arr(8), arr(6), arr(7), arr(16))
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.LEVEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.ONLINE_TIME)
    val ds: DataFrame = loginDs.union(logoutDs)
    ds
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val registerPattern = Constants.WAREHOUSE_DIR + "/ctpgsn/new_register/[yyyy-MM-dd]/*"
    val registerPath = PathUtils.generateLogPathDaily(registerPattern, logDate)
    registerRaw = getRawLog(registerPath)

    val filterlog = (line: String) => {
      var rs = false
      if (line.startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val registerDs = registerRaw.map(line => line.split("\\t")).filter(line => filterlog(line(0))).map { line =>
      val id = line(1)
      val dateTime = line(0)
      ("mp_cotyphu", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }
}
