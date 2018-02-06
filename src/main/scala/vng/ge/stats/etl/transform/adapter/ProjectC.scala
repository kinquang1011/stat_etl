package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, Formatter, TmpFormatter}
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

class ProjectC extends FairyFormatter("projectc") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPatternPath = Constants.GAMELOG_DIR + "/projectc/[yyyyMMdd]/pjc_[yyyyMMdd]_logdb/t_acct_water_*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath, logDate,1)
      paymentRaw = getRawLog(paymentPath)

      val filterLog = (line: Array[String]) => {
        line(11).toLong == 1 && line(10).toLong == 1 && line(9).toLong == 1 &&
          (MyUdf.timestampToDate((line(19).toLong) * 1000).startsWith(logDate))
      }

      val sf = Constants.FIELD_NAME


      var paymentDs = paymentRaw.map(line => line.split("\t"))
        .filter(line => filterLog(line))
        .map { line =>
          val datetime = MyUdf.timestampToDate((line(19).toLong) * 1000)
          val rid = line(3);
          val sid = line(6)
          val id = line(3)
          val money = line(15).toLong * 100
          ("projectc", datetime, rid, sid, id, money, money)
        }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.RID, sf.SID, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
      paymentDs

    } else {
      emptyDataFrame
    }
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logRaw: RDD[String] = null

    if (hourly == "") {
      val logPatternPath = Constants.GAMELOG_DIR + "/projectc/[yyyyMMdd]/pjc_[yyyyMMdd]_gamelog/65535_[yyyyMMdd]_*"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate,1)
      logRaw = getRawLog(logPath)
    } else {
      val loginPattern = Constants.GAME_LOG_DIR + "/projectc/[yyyyMMdd]/pjc_[yyyyMMdd]_gamelog/*.log"
      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      logRaw = getRawLog(loginPath)
    }

    val filterLog = (line: Array[String]) => {

      var rs = false

      if (
        ((line.length >= 29 && line(0) == "PlayerLogin") || (line.length >= 25 && line(0) == "PlayerLogout"))
          && line(2).startsWith(logDate)) {
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
    val logDs = logRaw.map(line => line.split("\\|")).filter(line => filterLog(line)).map { line =>


      val serverId = 1
      val datetime = line(2)
      val platform = getOs(line(4))
      var onlineTime = ""
      var action = ""
      var level = ""
      var ip = ""
      var network = ""
      var scrW = ""
      var scrH = ""
      var loginChannel = ""
      var roleName = ""
      var roleid = ""
      var deviceId = ""
      var playerId = ""

      if (line(0).startsWith("PlayerLogin")) {
        level = line(7)
        network = line(13)
        scrW = line(14)
        scrH = line(15)
        loginChannel = line(17)
        roleid = line(18)
        roleName = line(19)
        deviceId = line(24)
        playerId = line(25)
        ip = line(29)
        action = "login"

      } else {
        roleid = line(6)
        onlineTime = line(7)
        level = line(8)
        network = line(14)
        scrW = line(15)
        scrH = line(16)
        loginChannel = line(18)
        deviceId = line(23)
        playerId = line(24)
        roleName = line(25)
        action = "logout"
      }

      val resolution = scrW + "," + scrH


      ("projectc", datetime, serverId, platform, level, network, resolution, onlineTime, loginChannel, roleid, roleName,
        deviceId, playerId, ip, action)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.OS, sf.LEVEL, sf.NETWORK, sf.RESOLUTION, sf.ONLINE_TIME,
      sf.CHANNEL, sf.RID, sf.ROLE_NAME, sf.DID, sf.ID, sf.IP, sf.ACTION)
    logDs
  }

}
