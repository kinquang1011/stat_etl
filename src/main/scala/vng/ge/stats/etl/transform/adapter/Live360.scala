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
class Live360 extends FairyFormatter("360live") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAME_LOG_DIR + "/360live/[yyyy-MM-dd]/360LIVE_ACCESS/360LIVE_ACCESS-[yyyy-MM-dd].gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)

    } else {
      val loginPattern = Constants.GAME_LOG_DIR + "/360live/[yyyy-MM-dd]/360LIVE_ACCESS/360LIVE_ACCESS-[yyyy-MM-dd]_*"
      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
    }
    val filter = (s1: Array[String]) => {
      var rs = false
      if (s1(0).length != 0) {
        if ((MyUdf.timestampToDate(s1(0).toLong).startsWith(logDate)) && (s1.length >= 3)) {
          rs = true
        }
      }
      rs
    }

    val sc = Constants.FIELD_NAME
    val loginDS = loginRaw.map(line => line.split("\\t")).filter(line => filter(line)).map { r =>
      val logDate = MyUdf.timestampToDate(r(0).toLong)
      ("360live", logDate, r(2), r(1), r(3), "login")
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.IP, sc.OS, sc.ACTION)
    loginDS
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      var paymentPath: Array[String] = Array()
      if (logDate <= "2017-05-10") {
        paymentPath = Array("/ge/gamelogs/360live/2017-05-10/360live_payment_stats/360live_payment_stats-2017-05-10.gz")
      } else {
        val paymentPattern = Constants.GAME_LOG_DIR + "/360live/[yyyy-MM-dd]/360live_payment_stats/*.gz"
        paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
      }
      paymentRaw = getRawLog(paymentPath)
    } else {
      val paymentPattern = Constants.GAME_LOG_DIR + "/360live/[yyyy-MM-dd]/360live_payment_stats" +
        "/360live_payment_stats-[yyyy-MM-dd]_*"
      val paymentPath = PathUtils.generateLogPathHourly(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
    }

    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 10) {
        if (MyUdf.timestampToDate(line(0).toLong).startsWith(logDate) && !line(9).equalsIgnoreCase("GMTOOL")
          && !line(9).equalsIgnoreCase("TeenIdol")&& !line(9).startsWith("SYSTEM")) {
          rs = true
        }
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
    val convertMap = Map(
      "AUD" -> 16543.0,
      "VND" -> 1.0,
      "EUR" -> 24585.87,
      "USD" -> 22700.0,
      "GBP" -> 29099.24,
      "HKD" -> 2878.57,
      "JPY" -> 196.87,
      "KRW" -> 18.49,
      "SGD" -> 15925.7,
      "THB" -> 641.5,
      "CAD" -> 16336.68,
      "CHF" -> 22284.26
    )
    val getMoney = (money: String, currency: String) => {
      var vnd = 0.0
      if (convertMap.contains(currency)) {
        vnd = money.toDouble * convertMap(currency)
      }
      vnd
    }

    val sc = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => paymentFilter(line)).map { line =>
      val logDate = MyUdf.timestampToDate(line(0).toLong)
      val userId = line(1)
      val os = getOs(line(2))
      val gross_amt = getMoney(line(3), line(5))
      val net_amt = getMoney(line(4), line(5))
      ("360live", logDate, userId, os, gross_amt, net_amt)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.OS, sc.GROSS_AMT, sc.NET_AMT)
    paymentDs
  }
}
  //CCU LOG

  /*override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/mrtg/server_ccu.[yyyyMMdd]"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
      raw = getRawLog(logPath)
    }
    val gameFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 3
        && line(0).toLowerCase.startsWith("360live")
        && line(0).split("_").length == 1) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
      val ccu = line(1)
      val timeStamp = line(2).toLong
      val dateTime = MyUdf.timestampToDate(timeStamp * 1000)
      ("360live", ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
    ccuDs
  }*/


