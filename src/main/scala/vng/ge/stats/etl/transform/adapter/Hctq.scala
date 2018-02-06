package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 25/02/2017.
  */
//contact point : hieutm
//Code ccu: 281

class Hctq extends FairyFormatter("hctq") {

  import sqlContext.implicits._
  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs: DataFrame = null
    var logoutDs: DataFrame = null
    if (hourly == "") {
      val patternUserPath = Constants.GAMELOG_DIR + "/tqvs/[yyyyMMdd]/log/*/*_log_login_[yyyyMMdd]*.csv"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
      loginDs = getCsvWithHeaderLog(userPath, ",").select("serverid", "account", "ip", "gmlevel", "logintime")
      val logPatternPath = Constants.GAMELOG_DIR + "/tqvs/[yyyyMMdd]/log/*/*_log_logout_[yyyyMMdd]*.csv"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate)
      logoutDs = getCsvWithHeaderLog(logPath, ",").select("serverid", "account", "gmlevel", "logouttime", "onlinetime")
    }
    val sf = Constants.FIELD_NAME
    loginDs = loginDs.withColumnRenamed("serverid", sf.SID)
      .withColumnRenamed("account", sf.ID)
      .withColumnRenamed("gmlevel", sf.LEVEL)
      .withColumn(sf.LOG_DATE, col("logintime"))
      .withColumn("date", col("logintime").substr(0, 10))
      .withColumn(sf.ONLINE_TIME, lit(0)).drop("logintime")
    logoutDs = logoutDs.withColumnRenamed("serverid", sf.SID)
      .withColumnRenamed("account", sf.ID)
      .withColumn(sf.IP, lit(""))
      .withColumnRenamed("gmlevel", sf.LEVEL)
      .withColumn(sf.LOG_DATE, col("logouttime"))
      .withColumn("date", col("logouttime").substr(0, 10))
      .withColumnRenamed("onlinetime", sf.ONLINE_TIME).drop("logouttime")
    val ds = loginDs.select(sf.ID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.LOG_DATE, "date")
      .union(logoutDs.select(sf.ID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.LOG_DATE, "date"))
      .where(s"date like '$logDate'")
    ds.drop("date").withColumn(sf.GAME_CODE, lit("hctq"))

  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var paymentDs: DataFrame = null
    if (hourly == "") {
      val pattern = Constants.GAME_LOG_DIR + "/tqvs/[yyyyMMdd]/log/*/*_t_u_pay_order_[yyyyMMdd].csv"
      val paymentPath = PathUtils.generateLogPathDaily(pattern, logDate)
      paymentDs = getCsvWithHeaderLog(paymentPath, ",").filter($"PayDate".contains(logDate))
        .select("account", "Points", "PayDate", "ServerId","PaySuccess","CurrencyType")
    }
    paymentDs = paymentDs.withColumnRenamed("account", sf.ID)
    paymentDs = paymentDs.withColumn(sf.GROSS_AMT, col("Points") * 100)
    paymentDs = paymentDs.withColumn(sf.NET_AMT, col("Points") * 100)
    paymentDs = paymentDs.withColumnRenamed("ServerId", sf.SID)
    paymentDs = paymentDs.withColumnRenamed("PayDate", sf.LOG_DATE)
    paymentDs = paymentDs.withColumn(sf.GAME_CODE,lit("hctq"))
    paymentDs = paymentDs.where("PaySuccess == 1 and CurrencyType not like 'PW_INTERNAL_PAY' ")
    paymentDs
  }
}
