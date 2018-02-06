package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 10/02/2017.
  */
class Zingplaythai_Poker extends MyplayFormatter ("zingplaythai_poker"){
  import sqlContext.implicits._
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  def otherFunction(): Unit = {
    //countryMapping
    //moneyFlow
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == ""){
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_poker_thailan*"
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
      val money = line(4)
      val id = line(2)
      ("zingplaythai_poker", dateTime,id,money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT)
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var loginDsFromHive: DataFrame = null
    var logoutDsFromHive: DataFrame = null
    if(hourly == ""){
      val loginQuery = s"select * from zingplaythai_poker.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from zingplaythai_poker.logout where ds ='$logDate'"
      logoutDsFromHive = getHiveLog(logoutQuery)
    }
    var loginDs = loginDsFromHive.selectExpr("log_date as log_date","user_id as id","server_id as sid",
      "role_id as rid ","role_name as role_name","platform as os", "device_name as device",
      "level as level")
    loginDs = loginDs.withColumn("online_time",lit(0))
    loginDs = loginDs.withColumn("action",lit("login"))
    var logoutDs = logoutDsFromHive.selectExpr("log_date as log_date","user_id as id","server_id as sid",
      "role_id as rid ","role_name as role_name","platform as os", "device_name as device",
      "level as level","online_time as online_time")
    logoutDs = logoutDs.withColumn("action",lit("logout"))
    var ds = loginDs.union(logoutDs)
    ds = ds.withColumn("game_code",lit("zingplaythai_poker"))
    ds
  }
}
