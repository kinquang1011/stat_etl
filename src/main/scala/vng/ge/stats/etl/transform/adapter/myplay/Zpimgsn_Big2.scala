package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter

/**
  * Created by quangctn on 13/02/2017.
  */
class Zpimgsn_Big2 extends MyplayFormatter ("zpimgsn_big2") {
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  def otherFunction(): Unit = {
    //countryMapping
    //mone
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {

    var paymentFromHive: DataFrame = null
    if (hourly == ""){
      val paymentQuery = s"SELECT * FROM zpimgsn_big2.recharge where ds = '$logDate'"
      paymentFromHive = getHiveLog(paymentQuery)
    }
    var paymentDs = paymentFromHive.selectExpr("log_date as log_date", "account as id", "server_id as sid"
      ,"charge_coin_exist as gross_amt","charge_coin_exist as net_amt")
    paymentDs = paymentDs.withColumn("game_code",lit("zpimgsn_big2"))
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logUserFromHive: DataFrame = null
    if(hourly == ""){
      val logQuery = s"select * from zpimgsn_big2.user where ds = '$logDate'"
      logUserFromHive = getHiveLog(logQuery)
    }
    var logDs = logUserFromHive.selectExpr("log_date as log_date","user_id as id")
    logDs = logDs.withColumn("game_code",lit("zpimgsn_big2"))
    logDs
  }

}
