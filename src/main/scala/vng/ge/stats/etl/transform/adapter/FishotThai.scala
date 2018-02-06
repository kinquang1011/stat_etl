package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, PathUtils}

/**
  * Created by quangctn on 10/04/2017.
  * Contact point : thinhnd2
  */
class FishotThai extends FairyFormatter ("fishotthai") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val patternLogin = Constants.GAMELOG_DIR + "/fishotthai/[yyyyMMdd]/" +
        "loggame/login/login-[yyyy-MM-dd]*.gz"
      val patternLogout = Constants.GAMELOG_DIR + "/fishotthai/[yyyyMMdd]/" +
        "loggame/logout/logout-[yyyy-MM-dd]*.gz"
      val pathLogin = PathUtils.generateLogPathDaily(patternLogin, logDate)
      val pathLogout = PathUtils.generateLogPathDaily(patternLogout, logDate)
      loginRaw = getRawLog(pathLogin)
      logoutRaw = getRawLog(pathLogout)
    }
    val filterDummy = (s1: Array[String]) => {
      var rs = false
      if((s1(0) forall Character.isDigit) && (s1(0).length == 13)) {
        if ((MyUdf.timestampToDate(s1(0).toLong).startsWith(logDate)) && s1.length >= 10) {
          rs = true
        }
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\t")).filter(line => filterDummy(line)).map { s =>
      val dateTime = MyUdf.timestampToDate(s(0).toLong)
      val id = s(1)
      val sid = s(2)
      val rid = s(3)
      val roleName = s(4)
      val os = s(6)
      val osVers = s(7)
      val device = s(8)
      val level = s(10)
      ("fishotthai", dateTime, id, sid, rid, roleName, os, osVers, device, level, "login")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS,
      sf.OS_VERSION, sf.DEVICE, sf.LEVEL, sf.ACTION)


    val logoutDs = logoutRaw.map(line => line.split("\t")).filter(line => filterDummy(line)).map{s =>
      val dateTime = MyUdf.timestampToDate(s(0).toLong)
      val id = s(1)
      val sid = s(2)
      val rid = s(3)
      val roleName = s(4)
      val os = s(6)
      val osVers = s (7)
      val device = s(8)
      val level = s(10)
      ("fishotthai", dateTime, id, sid, rid, roleName, os, osVers, device, level,"logout")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS,
      sf.OS_VERSION, sf.DEVICE, sf.LEVEL,sf.ACTION)
    val ds = loginDs.union(logoutDs)
    ds
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPattern = Constants.GAME_LOG_DIR + "/" +
        "fishotthai/[yyyyMMdd]/logifrs/fishot_ifrs/*.gz"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    /*filter base on 2 condition
    1. Logdate
    2. gross_revenue != 0*/
    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if((line(3) forall Character.isDigit) && (line(3).length == 13)) {
        if (MyUdf.timestampToDate(line(3).toLong).startsWith(logDate)
          && line.length >= 13 && line(10).toLong != 0) {
          rs = true
        }
      }
      rs
    }

    val sc = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split(",")).filter(line => paymentFilter(line)).map { line =>
      val logDate = MyUdf.timestampToDate(line(3).toLong)
      val id = line(0)
      val sid = line(4)
      val gross_amt = line(10)
      val net_amt = line(11)
      val tid = line(13)
      ("fishotthai",logDate,sid,id, gross_amt, net_amt, tid)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.SID,sc.ID, sc.GROSS_AMT, sc.NET_AMT, sc.TRANS_ID)
    paymentDs
  }
  //CCU LOG
 /* override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/mrtg/server_ccu.[yyyyMMdd]"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
      raw = getRawLog(logPath)
    }
    val gameFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 3 && line(0).toLowerCase.startsWith("FTGFBS2")) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
      val ccu = line(1)
      val timeStamp = line(2).toLong
      val dateTime = MyUdf.timestampToDate(timeStamp.toLong*1000)
      ("fishotthai", ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
    ccuDs
  }*/

}
