package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by canhtq on 30/03/2017.
  */
class Phuclong extends FairyFormatter("phuclong") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close()
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginLogoutRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/nh/[yyyyMMdd]/*/login.*"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      loginLogoutRaw = getRawLog(logPath, true, "\\|")
    }

    val date = logDate.replaceAll("-", "")

    val sf = Constants.FIELD_NAME
    val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).map { line =>
      val data: String = line(1)
      val path = line(0)
      val its: Array[String] = data.split("\\t")
      val id = its.apply(0)
      val dateTime = MyUdf.timestampToDate(its.apply(2).toLong * 1000)
      val level = its.apply(1)
      val sid = MyUdf.getSid(path, date)

      ("phuclong", dateTime, "login", id, level, sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.LEVEL, sf.SID)
    loginLogoutDs.where(s"log_date like '%$logDate%'")
  }

  def getActivityDs2(logDate: String, hourly: String): DataFrame = {
    var loginLogoutRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/nh/[yyyyMMdd]/*/login.*"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      loginLogoutRaw = getRawLog(logPath, true, "\\|")
    }

    val date = logDate.replaceAll("-", "")

    val sf = Constants.FIELD_NAME
    val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).map { line =>
      val data: String = line(1)
      val path = line(0)
      val its: Array[String] = data.split("\\t")
      val id = its.apply(0)
      val dateTime = MyUdf.timestampToDate(its.apply(2).toLong * 1000)
      val level = its.apply(1)
      val sid = MyUdf.getSid(path, date)

      ("phuclong", dateTime, "login", id, level, sid, id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.LEVEL, sf.SID, sf.RID)
    loginLogoutDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var rechargRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/nh/[yyyyMMdd]/*/recharge.*"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      rechargRaw = getRawLog(logPath, true, "\\|")
    }
    val date = logDate.replaceAll("-", "")

    val sf = Constants.FIELD_NAME
    val rechargeDs = rechargRaw.map(line => line.split("\\|")).map { line =>
      val data: String = line(1)
      val path = line(0)
      val its: Array[String] = data.split("\\t")
      val rev = its.apply(3).toDouble * 100
      val dateTime = MyUdf.timestampToDate(its.apply(4).toLong * 1000)
      val id = its.apply(1)
      val tid = its.apply(0)
      val sid = MyUdf.getSid(path, date)
      ("phuclong", dateTime, id, rev, rev, tid, sid, id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.NET_AMT, sf.GROSS_AMT, sf.TRANS_ID, sf.SID, sf.RID)
    rechargeDs.dropDuplicates(sf.TRANS_ID).where(s"log_date like '%$logDate%'")
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var ccuRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/nh/[yyyyMMdd]/*/ccu.*"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      ccuRaw = getRawLog(logPath, true, "\\|")
    }

    val date = logDate.replaceAll("-", "")
    val sf = Constants.FIELD_NAME
    val ccuDs = ccuRaw.map(line => line.split("\\|")).map { line =>
      val data: String = line(1)
      val path = line(0)
      val its: Array[String] = data.split("\\t")

      val ccu = its.apply(1)
      val dateTime = MyUdf.timestampToDate(its.apply(0).toLong * 1000)
      val sid = MyUdf.getSid(path, date)
      ("phuclong", dateTime, sid, ccu)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.CCU)
    ccuDs
  }


}
