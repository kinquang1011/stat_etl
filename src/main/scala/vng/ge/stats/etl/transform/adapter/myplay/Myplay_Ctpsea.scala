package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter


/**
  * Created by quangctn on 14/02/2017.
  */

class Myplay_Ctpsea extends MyplayFormatter("myplay_ctpsea") {

  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }


  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDsFromHive: DataFrame = null
    var logoutDsFromHiv: DataFrame = null
    if (hourly == "") {
      val loginQuery = s"select * from ctpsea.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from ctpsea.logout where ds ='$logDate'"
      logoutDsFromHiv = getHiveLog(logoutQuery)
    }
    val loginDs = loginDsFromHive.selectExpr("log_date as log_date"
        ,"user_name as id"
        ,"server_id as sid"
        ,"level as level"
        ,"device_name as device"
        ,"os_platform as os"
        ,"os_version as os_version"
        ,"'0' as online_time")
    val logoutDs = logoutDsFromHiv.selectExpr("log_date as log_date"
      , "user_name as id"
      , "server_id as sid"
      ,"level as level"
      ,"device_name as device"
      ,"os_platform as os"
      ,"os_version as os_version"
      ,"total_online_second as online_time")
    var logDs = loginDs.union(logoutDs)
    logDs = logDs.withColumn("game_code", lit("myplay_ctpsea"))
    logDs

  }
}
