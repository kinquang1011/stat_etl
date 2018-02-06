package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, PathUtils}

import scala.util.matching.Regex

/**
  * Created by quangctn on 05/05/2017.
  * Contact point Thinh Nguyen Duc (2) for this game
  */

class Tfirb2s extends FairyFormatter("tfirb2s") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    val patternUserPath = Constants.GAMELOG_DIR + "/tfirb2s/[yyyy-MM-dd]/Login/Login*"
    val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate, 1)
    val logPatternPath = Constants.GAMELOG_DIR + "/tfirb2s/[yyyy-MM-dd]/Logout/Logout*"
    val logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate, 1)
    loginRaw = getRawLog(userPath)
    logoutRaw = getRawLog(logPath)


    val filterLog = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(line(0).toLong).startsWith(logDate) && line.length >= 17) {
        rs = true
      }
      rs
    }
    val getPlatform = (os1: String) => {
      var result = "other"
   /*   Common.logger(os1)*/
      if (os1 != null) {

        val os = os1.toLowerCase

        if (os.contains("android")) {
          result = "android"
        } else if (os.contains("ios")) {
          result = "ios"
        }
        result
      } else {
        os1
      }
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val rid = line(3)
      val roleName = line(4)
      val os = getPlatform(line(6))
      val did = line(8)
      val deviceName = line(9)
      val level = line(11)
      var online = "0"
      ("tfirb2s", datetime, id, sid, rid, roleName, os, did, deviceName, level, online, "login")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.DID, sf.DEVICE, sf.LEVEL, sf.ONLINE_TIME, sf.ACTION)
    val logoutDs = logoutRaw.map(line => line.split("\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val rid = line(3)
      val roleName = line(4)
      val os = getPlatform(line(6))
      val did = line(8)
      val deviceName = line(9)
      val level = line(11)
      val online = line(17)
      ("tfirb2s", datetime, id, sid, rid, roleName, os, did, deviceName, level, online, "logout")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.DID, sf.DEVICE, sf.LEVEL, sf.ONLINE_TIME, sf.ACTION)
    loginDs.union(logoutDs)

  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/tfirb2s/[yyyy-MM-dd]/Paying/Paying*"
      val paymentPath = PathUtils.generateLogPathDaily(patternPath, logDate, 1)
      paymentRaw = getRawLog(paymentPath)
    } else {
      val patternPath = Constants.GAMELOG_DIR + "/tfirb2s/[yyyy-MM-dd]/Paying/Paying-[yyyy-MM-dd]_*"
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

      ("tfirb2s", datetime, rid, id, sid, tid, channel, gross_amt, net_amt, ip)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.RID, sf.ID, sf.SID, sf.TRANS_ID, sf.CHANNEL, sf.GROSS_AMT, sf.NET_AMT, sf.IP)
    paymentDs
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val regPatternPath = Constants.GAMELOG_DIR + "/tfirb2s/[yyyy-MM-dd]/Register/Register*"
    val userPath = PathUtils.generateLogPathDaily(regPatternPath, logDate, 1)
    registerRaw = getRawLog(userPath)

    val filterLog = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(line(0).toLong).startsWith(logDate) && line.length >= 17) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val regDs = registerRaw.map(line => line.split("\t")) /*.filter(line => filterLog(line))*/ .map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val sid = line(2)
      val os = line(4)
      val os_version = line(5)
      val did = line(6)
      val deviceName = line(7)
      ("tfirb2s", datetime, id, sid, os, os_version, did, deviceName)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.OS, sf.OS_VERSION, sf.DID, sf.DEVICE)
    regDs
  }
}

