package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 15/08/2017.
  * Contact point : tuanntt@vng.com.vn
  * CCU Flume : 286
  * Alpha Test: 23/10/2017
  */
class Pntt extends FairyFormatter("pntt") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  private val createUID = udf { (userId: String, sid: String) => {
    val id = userId + "." + sid
    id
  }
  }


  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: DataFrame = null
    var logoutRaw: DataFrame = null
    val sf = Constants.FIELD_NAME
    if (hourly == "") {
      val patternUserPath = Constants.GAMELOG_DIR + "/pnttmobi/[yyyy-MM-dd]/datalog/gamelog_[yyyyMMdd]/log_login/log_login.txt.*"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate, 1)
      loginRaw = getCsvLog(userPath, "\t").select("_c0", "_c3", "_c4", "_c5", "_c8")
      val logPatternPath = Constants.GAMELOG_DIR + "/pnttmobi/[yyyy-MM-dd]/datalog/gamelog_[yyyyMMdd]/log_logout/log_logout.txt.*"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate, 1)
      logoutRaw = getCsvLog(logPath, "\t").select("_c0", "_c3", "_c4", "_c5", "_c8", "_c11")

    }
    loginRaw = loginRaw.withColumnRenamed("_c3", sf.LOG_DATE)
      .withColumnRenamed("_c0", sf.OS)
      .withColumnRenamed("_c4", sf.SID)
      .withColumnRenamed("_c5", "userId")
      .withColumnRenamed("_c8", sf.IP)
      .withColumn(sf.ONLINE_TIME, lit(0))
      .withColumn(sf.ID, createUID(col("userId"),col("sid")))
      .withColumn(sf.ACTION, lit("login"))

    logoutRaw = logoutRaw.withColumnRenamed("_c3", sf.LOG_DATE)
      .withColumnRenamed("_c0", sf.OS)
      .withColumnRenamed("_c4", sf.SID)
      .withColumnRenamed("_c5", "userId")
      .withColumnRenamed("_c8", sf.IP)
      .withColumnRenamed("_c11", sf.ONLINE_TIME)
      .withColumn(sf.ID, createUID(col("userId"),col("sid")))
      .withColumn(sf.ACTION, lit("logout"))

    val logDs = loginRaw.union(logoutRaw).select(sf.OS,sf.LOG_DATE, sf.ID, sf.SID, sf.ONLINE_TIME, sf.IP, sf.ACTION)
      .withColumn(sf.GAME_CODE, lit("pntt")).where(s"log_date like '%$logDate%'")
    logDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var paymentRaw: DataFrame = null
    val pathPattern = Constants.GAMELOG_DIR + "/pnttmobi/[yyyy-MM-dd]/datalog/gamelog_[yyyyMMdd]/" +
      "log_recharge/log_recharge.txt.*"
    val realPath = PathUtils.generateLogPathDaily(pathPattern, logDate, 1)
    paymentRaw = getCsvLog(realPath, "\t")

    if(paymentRaw!= emptyDataFrame){
      paymentRaw=paymentRaw.select("_c3", "_c0", "_c4", "_c5", "_c8", "_c10")
      paymentRaw.withColumnRenamed("_c3", sf.LOG_DATE)
        .withColumnRenamed("_c0", sf.OS)
        .withColumnRenamed("_c4", sf.SID)
        .withColumnRenamed("_c5", "userId")
        .withColumnRenamed("_c8", sf.IP)
        .withColumn(sf.NET_AMT, col("_c10"))
        .withColumnRenamed("_c10", sf.GROSS_AMT)
        .withColumn(sf.GAME_CODE, lit("pntt"))
        .withColumn(sf.ID, createUID(col("userId"),col("sid")))
        .where(sf.LOG_DATE + s" like '%$logDate%'")
    }
    paymentRaw
  }
}

