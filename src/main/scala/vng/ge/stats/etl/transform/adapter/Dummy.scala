package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 10/04/2017.
  */
class Dummy extends FairyFormatter ("dummy"){
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val patternLogin = Constants.GAMELOG_DIR + "/zingplaythai/[yyyy-MM-dd]/" +
        "logparser/game_buffer_{dummy,lieng,poker}/{dummy,lieng,poker}_metric_login/*.gz"
      val patternLogout = Constants.GAMELOG_DIR + "/zingplaythai/[yyyy-MM-dd]/" +
        "logparser/game_buffer_{dummy,lieng,poker}/{dummy,lieng,poker}_metric_logout/*.gz"
      val pathLogin = PathUtils.generateLogPathDaily(patternLogin, logDate)
      val pathLogout = PathUtils.generateLogPathDaily(patternLogout, logDate)
      loginRaw = getRawLog(pathLogin)
      logoutRaw = getRawLog(pathLogout)
    }else{
      val patternLogin = Constants.GAMELOG_DIR + "/zingplaythai/[yyyy-MM-dd]/" +
        "logparser/game_buffer_{dummy,lieng,poker}/{dummy,lieng,poker}_metric_login/" +
        "{dummy,lieng,poker}_metric_login-[yyyy-MM-dd]_*"

      val patternLogout = Constants.GAMELOG_DIR + "/zingplaythai/[yyyy-MM-dd]/" +
        "logparser/game_buffer_{dummy,lieng,poker}/{dummy,lieng,poker}_metric_logout/" +
        "{dummy,lieng,poker}_metric_logout-[yyyy-MM-dd]_*"
      val pathLogin = PathUtils.generateLogPathHourly(patternLogin, logDate)
      val pathLogout = PathUtils.generateLogPathHourly(patternLogout, logDate)
      loginRaw = getRawLog(pathLogin)
      logoutRaw = getRawLog(pathLogout)
    }
    val filterDummy = (s1: Array[String]) => {
      var rs = false
      if ((MyUdf.timestampToDate(s1(0).toLong*1000).startsWith(logDate)) && s1.length >=10) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\t")).filter(line => filterDummy(line)).map{s =>
      val dateTime = MyUdf.timestampToDate(s(0).toLong*1000)
      val id = s(1)
      val sid = s(2)
      val rid = s(3)
      val roleName = s(4)
      val os = s(6)
      val osVers = s (7)
      val device = s(8)
      val level = s(10)
      ("dummy", dateTime, id, sid, rid, roleName, os, osVers, device, level,"login")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS,
      sf.OS_VERSION, sf.DEVICE, sf.LEVEL,sf.ACTION)

    val logoutDs = logoutRaw.map(line => line.split("\t")).filter(line => filterDummy(line)).map{s =>
      val dateTime = MyUdf.timestampToDate(s(0).toLong*1000)
      val id = s(1)
      val sid = s(2)
      val rid = s(3)
      val roleName = s(4)
      val os = s(6)
      val osVers = s (7)
      val device = s(8)
      val level = s(10)
      ("dummy", dateTime, id, sid, rid, roleName, os, osVers, device, level,"logout")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS,
      sf.OS_VERSION, sf.DEVICE, sf.LEVEL,sf.ACTION)
    val ds = loginDs.union(logoutDs)
    ds
  }
  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPattern = Constants.GAME_LOG_DIR + "/" +
        "zingplaythai/[yyyy-MM-dd]/ifrs_buffer_{dummy,lieng,poker}/*/*.gz"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
    }else{
      val paymentPattern = Constants.GAME_LOG_DIR + "/" +
        "zingplaythai/[yyyy-MM-dd]/ifrs_buffer_{dummy,lieng,poker}/*/{lieng,poker}_ifrs_*"
      val paymentPath = PathUtils.generateLogPathHourly(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
    }

    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(line(3).toLong).startsWith(logDate)
        && line.length >=15 && line(10).toLong !=0) {
        rs = true
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
      val rid = line(14)
      ("dummy",logDate,sid,id, gross_amt, net_amt, tid, rid)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.SID,sc.ID, sc.GROSS_AMT, sc.NET_AMT, sc.TRANS_ID, sc.RID)
    paymentDs.dropDuplicates(sc.TRANS_ID)
  }

  //CCU LOG
  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions._
    var raw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/zingplaythai/[yyyy-MM-dd]/log_ccu/" +
        "{poker,lieng,dummy}_ccu/[yyyyMMdd]/*_metric_ccu/*.gz"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
      raw = getRawLog(logPath)
    }
    val Filter = (line: Array[String]) => {
      var rs = false
      if (line.length == 3) {
        rs = true
      }
      rs
    }

    //mm:1,vi:859,international:25,my:4,th:106
    val getValue =(line : Array[String]) => {
      var totalCcu = 0
      //mm:1
      for (x <- line) {
        if (x.split(":").length == 2) {
          totalCcu += (x.split(":")(1)).toInt
        }
      }
      totalCcu
    }
    val sf = Constants.FIELD_NAME
    var ccuDs = raw.map(line => line.split("\\t")).filter(line => Filter(line)).map { line =>
      val serverId = line(1)
      val timeStamp = line(0)
      val arrCcu = line(2).split(",")
      val ccu = getValue(arrCcu)
      ("dummy", serverId, ccu, timeStamp)
    }.toDF(sf.GAME_CODE, sf.SID, sf.CCU, "date")
    ccuDs = ccuDs.withColumn(sf.LOG_DATE,MyUdf.roundCcuDummy(col("date")))
    ccuDs
  }
}
//DUMMYB2S
