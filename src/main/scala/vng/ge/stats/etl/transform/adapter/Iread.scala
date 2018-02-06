package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, PathUtils}

/**
  * Created by quangctn on 11/01/2018
  * Contact point : datlc,lybn
  * Release Date : 24/12
  * CCU code : 289 ---Ko do duoc CCU
  */
class Iread extends FairyFormatter("iread") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logRaw: RDD[String] = null
    if (hourly == "") {
      val patternUserPath = Constants.GAMELOG_DIR + "/iread/[yyyy-MM-dd]/datalog/LOGIN_SERVICE*"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
      logRaw = getRawLog(userPath)
    }
    //Action id = 1 Login, =2: Logout, =4: Login Web
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length >= 13) {
        if ((MyUdf.timestampToDate((line(0)).toLong * 1000).startsWith(logDate)) &&
          !(line(6).equalsIgnoreCase("0")) &&
          (line(1).equalsIgnoreCase("1") || line(1).equalsIgnoreCase("2") || line(1).equalsIgnoreCase("4"))) {
          rs = true
        }
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s.toLowerCase().trim.contains("android")) {
        platform = "android"
      } else if (s.toLowerCase.trim.contains("ios")) {
        platform = "ios"
      }
      platform
    }
    val getAction = (s: String) => {
      var action = "logout"
      if (s.equalsIgnoreCase("1") || s.equalsIgnoreCase("4")) {
        action = "login"
      }
      action
    }

    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong * 1000)
      val uid = line(6)
      val ip = line(8)
      val os = getOs(line(11))
      val action = getAction(line(1))
      /* val moreInfo = line(12)
       val mapper = new ObjectMapper() with ScalaObjectMapper
       mapper.registerModule(DefaultScalaModule)
       val obj = mapper.readValue[Map[String, Object]](moreInfo)
       var sid = "";
       if (obj.keys.equals("sid")) {
          sid = obj("sid").toString
       }*/
      ("iread", datetime, uid, ip, os, action, ip /*, sid*/ )
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.IP, sf.OS, sf.ACTION, sf.IP /*, sf.SID*/)
    logDs
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var logRaw: RDD[String] = null
    val patternUserPath = Constants.GAMELOG_DIR + "/iread/[yyyy-MM-dd]/datalog/LOGIN_SERVICE*"
    val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
    logRaw = getRawLog(userPath)
    //Action id = 1 Login, =2: Logout, =4: Login Web
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length >= 13) {
        if ((MyUdf.timestampToDate((line(0)).toLong * 1000).startsWith(logDate)) &&
          !(line(6).equalsIgnoreCase("0")) &&
          (line(1).equalsIgnoreCase("3") || line(1).equalsIgnoreCase("5"))) {
          rs = true
        }
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s.toLowerCase().trim.contains("android")) {
        platform = "android"
      } else if (s.toLowerCase.trim.contains("ios")) {
        platform = "ios"
      }
      platform
    }


    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong * 1000)
      val uid = line(6)
      val ip = line(8)
      val os = getOs(line(11))
      /* val moreInfo = line(12)
       val mapper = new ObjectMapper() with ScalaObjectMapper
       mapper.registerModule(DefaultScalaModule)
       val obj = mapper.readValue[Map[String, Object]](moreInfo)
       var sid = "";
       if (obj.keys.equals("sid")) {
          sid = obj("sid").toString
       }*/
      ("iread", datetime, uid, ip, os, ip /*, sid*/ )
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.IP, sf.OS, sf.IP /*, sf.SID*/)
    logDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var logRaw: RDD[String] = null
    if (hourly == "") {
      val patternUserPath = Constants.GAMELOG_DIR + "/iread/[yyyy-MM-dd]/datalog/PAYMENT_CONNECTOR/PAYMENT_CONNECTOR*"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
      logRaw = getRawLog(userPath)
    }
    //Action id = 1 Login, =2: Logout, =4: Login Web
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length >= 14) {
        if ((MyUdf.timestampToDate((line(0)).toLong * 1000).startsWith(logDate)) &&
          (line(1).equalsIgnoreCase("2"))) {
          rs = true
        }
      }
      rs
    }

    val getOs = (s: String) => {
      var platform = "other"
      if (s.toLowerCase().trim.contains("android")) {
        platform = "android"
      } else if (s.toLowerCase.trim.contains("ios")) {
        platform = "ios"
      }
      platform
    }
    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(0).toLong * 1000)
      val uid = line(6)
      val net_amt = line(7)
      val gross_amt = net_amt
      val tid = line(8)
      val ip = line(10)
      val os = getOs(line(12))

      ("iread", datetime, uid, net_amt, gross_amt, tid, ip, os)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.NET_AMT, sf.GROSS_AMT, sf.TRANS_ID, sf.IP, sf.OS)
    logDs
  }
}
