package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 09/02/2017.
  */
class Myplay_Coup extends MyplayFormatter ("myplay_coup"){
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == ""){
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_coup*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath,logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    val filterLog = (line: String) =>{
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val gross_amt = line(4)
      val id = line(2)
      val net_amt = gross_amt.toDouble * line(9).toDouble
      ("myplay_coup", dateTime,id,gross_amt, net_amt)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if(hourly == ""){
      val loginPatternPath = Constants.WAREHOUSE_DIR + "/myplay_coup/player_login/[yyyy-MM-dd]/*"
      val loginPath = PathUtils.generateLogPathDaily(loginPatternPath,logDate)
      loginRaw = getRawLog(loginPath)
      val logoutPatternPath = Constants.WAREHOUSE_DIR + "/myplay_coup/player_logout/[yyyy-MM-dd]/*"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPatternPath,logDate)
      logoutRaw = getRawLog(logoutPath)
    }
    val filterlog = (line:String) => {
      var rs = false
      if (line.startsWith(logDate)){
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
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line(0))).map { line =>
      val platform = getOs(line(6))
      val dateTime = line(0)
      val id = line(1)
      val channel = line(3)
      val ip = line(14)
      val action = "login"
      ("myplay_coup", dateTime, action,channel ,id, platform,ip)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION,sf.CHANNEL, sf.ID,sf.OS,sf.IP)
    val logoutDs = logoutRaw.map(line => line.split("\\t")).filter(line => filterlog(line(0))).map { line =>
      val platform = getOs(line(6))
      val dateTime = line(0)
      val id = line(1)
      val channel = line(3)
      val ip = line(14)
      val action = "logout"
      ("myplay_coup", dateTime, action, channel, id, platform, ip)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION,sf.CHANNEL, sf.ID,sf.OS,sf.IP)
    val ds: DataFrame = loginDs.union(logoutDs)
    ds
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val registerPattern = Constants.WAREHOUSE_DIR + "/myplay_coup/new_register/[yyyy-MM-dd]/*"
    val registerPath = PathUtils.generateLogPathDaily(registerPattern, logDate)
    registerRaw = getRawLog(registerPath)

    val filterlog = (line: String) => {
      var rs = false
      if (line.startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val registerDs = registerRaw.map(line => line.split("\\t")).filter(line => filterlog(line(0))).map { line =>
      val id = line(1)
      val dateTime = line(0)
      ("myplay_coup", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }

}
