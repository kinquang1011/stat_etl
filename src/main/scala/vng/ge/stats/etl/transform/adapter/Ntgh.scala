package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, Formatter}
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 19/01/2017.
  */
class Ntgh extends FairyFormatter("ntgh") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logoutRaw: RDD[String] = null
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAME_LOG_DIR + "/vhw/[yyyy-MM-dd]/*/[yyyy-MM-dd]_LOGIN_LOG.log.gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
      val logoutattern = Constants.GAME_LOG_DIR + "/vhw/[yyyy-MM-dd]/*/[yyyy-MM-dd]_LOGOUT_LOG.log.gz"
      val logoutPath = PathUtils.generateLogPathDaily(logoutattern, logDate)
      logoutRaw = getRawLog(logoutPath)
    }
    val filter = (s1: Array[String]) => {
      var rs = false
      if (s1(0).length != 0) {
        if ((MyUdf.timestampToDate(s1(8).toLong * 1000).startsWith(logDate)) && (s1.length >= 10)) {
          rs = true
        }
      }
      rs
    }


    val sc = Constants.FIELD_NAME
    val loginDS = loginRaw.map(line => line.split(" ")).filter(line => filter(line)).map { r =>
      val date = MyUdf.timestampToDate(r(8).toLong * 1000)
      val onlineTime = ""
      ("ntgh", date, r(3), r(10), r(7), "login", r(1), r(5), r(6), r(9), onlineTime)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.IP, sc.OS, sc.ACTION, sc.SID, sc.RID, sc.ROLE_NAME, sc.LEVEL, sc.ONLINE_TIME)

    val logoutDS = loginRaw.map(line => line.split(" ")).filter(line => filter(line)).map { r =>
      val date = MyUdf.timestampToDate(r(8).toLong * 1000)
      ("ntgh", date, r(3), r(10), r(7), "logout", r(1), r(5), r(6), r(9), r(12))
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.IP, sc.OS, sc.ACTION, sc.SID, sc.RID, sc.ROLE_NAME, sc.LEVEL, sc.ONLINE_TIME)
    val ds = loginDS.union(logoutDS)
    ds
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPattern = Constants.GAME_LOG_DIR + "/vhw/[yyyy-MM-dd]/*/[yyyy-MM-dd]_RECHARGE_LOG.log.gz"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    val listSidTest = Array("9997")
    val paymentFilter = (line: Array[String],lstSid: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(line(12).toLong * 1000).startsWith(logDate)) {
        rs = true
        for(i <- 0 until lstSid.length){
          if(line(1).equalsIgnoreCase(listSidTest(i))){
            rs = false;
          }
        }
      }
      rs
    }
    val sc = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split(" ")).filter(line => paymentFilter(line,listSidTest)).map { line =>
      val date = MyUdf.timestampToDate(line(12).toLong * 1000)
      val userId = line(3)
      val money = line(11).toLong *2.5 * 100
      val idPlatform = line(7)
      val sid = line(1)
      val rid = line(5)
      ("ntgh", date, userId, money, money, idPlatform,sid,rid)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.NET_AMT, sc.GROSS_AMT, sc.OS, sc.SID,sc.RID)
    paymentDs
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var ccuRaw: RDD[String] = null
    if (hourly == "") {
      val ccuPattern = Constants.GAME_LOG_DIR + "/vhw/[yyyy-MM-dd]/*/[yyyy-MM-dd]_ONLINE_LOG.log.gz"
      val ccuPath = PathUtils.generateLogPathDaily(ccuPattern, logDate)
      ccuRaw = getRawLog(ccuPath)
    }
    val filter = (s1: Array[String]) => {
      var rs = false
      if (s1(0).length != 0) {
        if ((MyUdf.timestampToDate(s1(8).toLong * 1000).startsWith(logDate)) && (s1.length >= 10)) {
          rs = true
        }
      }
      rs
    }
    val sc = Constants.FIELD_NAME
    val ccuDs = ccuRaw.map(line => line.split(" ")).filter(line => filter(line)).map { line =>
      val date = MyUdf.timestampToDate(line(8).toLong * 1000)
      val userId = line(3)
      val sid = line(1)
      val ccu = line(9)
      ("ntgh", date, sid, ccu)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.SID, sc.CCU)
    ccuDs
  }
}
