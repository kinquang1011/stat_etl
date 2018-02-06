package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, Formatter}
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 06/02/2017.
  */
class Ztm extends FairyFormatter ("ztm"){
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginLogoutRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/ztm/[yyyyMMdd]/logs/*/functiontlog*"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      loginLogoutRaw = getRawLog(logPath)
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

    val filterLoginLogout = (line:Array[String]) => {
      var rs = false
      if ((line(0) == "PlayerLogin" || line(0) == "PlayerLogout") && line.length >= 34 && line(2).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).filter(line => filterLoginLogout(line)).map { line =>
      val platform = getOs(line(4))
      val dateTime = line(2)
      val sid = line(5)
      val id = line(6)
      var online_time = ""
      var level = ""
      var device = ""
      var action = ""
      var rid = ""
      var did = ""
      var ip = ""
      if (line(0) == "PlayerLogin") {
        online_time = "0"
        level = line(7)
        device = line(11)
        rid = line(18)
        did = line(24)
        ip = line(28)
        action = "login"
      } else {
        online_time = line(7)
        level = line(8)
        device = line(12)
        rid = line(24)
        did = line(23)
        ip = line(27)
        action = "logout"
      }
      ("ztm", dateTime, action, id, rid, sid, online_time, level, platform,ip, device)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.ONLINE_TIME, sf.LEVEL, sf.OS,sf.IP ,sf.DEVICE)
    loginLogoutDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPattern = Constants.GAME_LOG_DIR + "/ztm/[yyyyMMdd]/logs/*/scenetlog*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
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
    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 14) {
        if (line(0) == "RechargeFlow" && line(2).startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }

    val sc = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\|")).filter(line => paymentFilter(line)).map { line =>
      val platform = getOs(line(4))
      val netRev = line(14).toLong * 20 // 20 thay vi 2000
      val grossRev = netRev
      val dateTime = line(2)
      val sid = line(5)
      val rid = line(7)
      val id = line(6)
      ("ztm", dateTime, id, rid, sid, platform, netRev, grossRev)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.RID, sc.SID, sc.OS, sc.NET_AMT, sc.GROSS_AMT)
    paymentDs
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var CcuRaw :RDD[String] = null
    if (hourly == ""){
      val patternPath = Constants.GAMELOG_DIR +  "/ztm/[yyyyMMdd]/ccu/ccu_[yyyyMMdd].csv"
      val CcuPath = PathUtils.generateLogPathDaily(patternPath,logDate)
      CcuRaw = getRawLog(CcuPath)
    }
    val filterPayTime = (line: Array[String]) => {
      var rs = false
      if (line.length>=4) {
        if (MyUdf.timestampToDate((line(4).toLong) * 1000).startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    var CcuDs = CcuRaw.map(line => line.split("\\t")).filter(line => filterPayTime(line)).map{ r =>
      val timeStamps = r(4).toLong
      val ccu = r(3)
      val sid = r(0)
      val datetime = MyUdf.timestampToDate(timeStamps*1000)
      ("ztm", datetime, ccu, sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.CCU, sf.SID)
    CcuDs
  }
}
