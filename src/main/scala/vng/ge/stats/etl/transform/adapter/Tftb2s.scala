package vng.ge.stats.etl.transform.adapter

import scala.util.matching.Regex
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, PathUtils}

/**
  * Created by quangctn on 05/05/2017.
  * Contact point Thinh Nguyen Duc (2) for this game
  */

class Tftb2s extends FairyFormatter("tftb2s") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly != "") {
      val patternUserPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Login/Login-[yyyy-MM-dd]_*"
      val userPath = PathUtils.generateLogPathHourly(patternUserPath, logDate)
      val logPatternPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Logout/Logout-[yyyy-MM-dd]_*"
      val logPath = PathUtils.generateLogPathHourly(logPatternPath, logDate)
      loginRaw = getRawLog(userPath)
      logoutRaw = getRawLog(logPath)
    } else {
      val patternUserPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Login/Login*.gz"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
      val logPatternPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Logout/Logout*.gz"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate)
      loginRaw = getRawLog(userPath)
      logoutRaw = getRawLog(logPath)
    }

    val filterLog = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(line(0).toLong).startsWith(logDate) && line.length >= 17) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val rid = line(3)
      val roleName = line(4)
      val os = line(6)
      val did = line(8)
      val deviceName = line(9)
      val level = line(11)
      var online = "0"
      ("tftb2s", datetime, id, sid, rid, roleName, os, did, deviceName, level, online, "login")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.DID, sf.DEVICE, sf.LEVEL, sf.ONLINE_TIME, sf.ACTION)

    val logoutDs = logoutRaw.map(line => line.split("\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val rid = line(3)
      val roleName = line(4)
      val os = line(6)
      val did = line(8)
      val deviceName = line(9)
      val level = line(11)
      val online = line(17)
      ("tftb2s", datetime, id, sid, rid, roleName, os, did, deviceName, level, online, "logout")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.DID, sf.DEVICE, sf.LEVEL, sf.ONLINE_TIME, sf.ACTION)
    loginDs.union(logoutDs)

  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Paying/Paying*.gz"
      val paymentPath = PathUtils.generateLogPathDaily(patternPath, logDate)
      paymentRaw = getRawLog(paymentPath)
    } else {
      val patternPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Paying/Paying-[yyyy-MM-dd]_*"
      val paymentPath = PathUtils.generateLogPathHourly(patternPath, logDate)
      paymentRaw = getRawLog(paymentPath)
    }

    val filterPayTime = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate((line(0)).toLong).startsWith(logDate)
        && line(9) != "0") {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\t")).filter(line => filterPayTime(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val rid = line(3)
      val tid = line(7)
      val channel = line(6)
      val gross_amt = line(9)
      val net_amt = line(10)
      val ipRegex = new Regex("(\\d[0-9\\.]*)")
      val ip = ipRegex.findFirstIn(line(8))

      ("tftb2s", datetime, rid, id, sid, tid, channel, gross_amt, net_amt, ip)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.RID, sf.ID, sf.SID, sf.TRANS_ID, sf.CHANNEL, sf.GROSS_AMT, sf.NET_AMT, sf.IP)
    paymentDs
  }

  /*override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/mrtg/server_ccu.[yyyyMMdd]"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
      raw = getRawLog(logPath)
    }
    val gameFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 3 && line(0).toLowerCase.startsWith("tftb2s")) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
      val ccu = line(1)
      val timeStamp = line(2).toLong
      val dateTime = MyUdf.timestampToDate(timeStamp * 1000)
      ("tftb2s", ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
    ccuDs
  }*/

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    if(_hourly == "") {
      Common.logger("KIN" + _hourly)
      val regPatternPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Register/Register*.gz"
      val userPath = PathUtils.generateLogPathDaily(regPatternPath, logDate)
      registerRaw = getRawLog(userPath)
    }else{
      Common.logger("QUANG" + _hourly)
      val regPatternPath = Constants.GAMELOG_DIR + "/tftb2s/[yyyy-MM-dd]/Register/Register-[yyyy-MM-dd]_*"
      val userPath = PathUtils.generateLogPathHourly(regPatternPath, logDate)
      registerRaw = getRawLog(userPath)
    }
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(line(0).toLong).startsWith(logDate) && line.length >= 17) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val regDs = registerRaw.map(line => line.split("\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val os = line(4)
      val os_version = line(5)
      val did = line(6)
      val deviceName = line(7)
      ("tftb2s", datetime, id, sid, os, os_version, did, deviceName)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.OS, sf.OS_VERSION, sf.DID, sf.DEVICE)
    regDs
  }
}

