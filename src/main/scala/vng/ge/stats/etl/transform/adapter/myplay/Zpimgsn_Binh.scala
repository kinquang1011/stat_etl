package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter


/**
  * Created by quangctn on 14/02/2017.
  */

class Zpimgsn_Binh extends MyplayFormatter ("zpimgsn_binh") {
  import org.apache.spark.sql.functions.lit
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  def otherFunction(): Unit = {
    //countryMapping
    //moneyFlow
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentFromHive: DataFrame = null
    if (hourly == ""){
      val paymentQuery = s"SELECT * FROM zpimgsn_binh.recharge where ds = '$logDate'"
      paymentFromHive = getHiveLog(paymentQuery)
    }
    var paymentDs = paymentFromHive.selectExpr("log_date as log_date", "account as id", "server_id as sid"
    ,"charge_coin_exist as gross_amt","charge_coin_exist as net_amt")
    paymentDs = paymentDs.withColumn("game_code",lit("zpimgsn_binh"))
      /*
      * Formula : Charge_coin existed * 182
      * Gross = net
      */
    paymentDs = paymentDs.withColumn("gross_amt",paymentDs("gross_amt")*182)
    paymentDs = paymentDs.withColumn("net_amt",paymentDs("gross_amt"))
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDsFromHive :DataFrame = null
    var logoutDsFromHiv :DataFrame = null
    if (hourly == ""){
      val loginQuery = s"select * from zpimgsn_binh.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from zpimgsn_binh.logout where ds ='$logDate'"
      logoutDsFromHiv = getHiveLog(logoutQuery)
    }
    val loginDs = loginDsFromHive.selectExpr("log_date as log_date", "user_id as id", "ip_addr as ip")
    val logoutDs =  logoutDsFromHiv.selectExpr("log_date as log_date", "user_id as id", "ip_addr as ip")
    var logDs = loginDs.union(logoutDs)
    logDs = logDs.withColumn("game_code", lit("zpimgsn_binh"))
    logDs

  }
}
