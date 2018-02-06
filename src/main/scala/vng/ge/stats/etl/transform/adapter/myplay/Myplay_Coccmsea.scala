package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter


/**
  * Created by quangctn on 14/02/2017.
  */

class Myplay_Coccmsea extends MyplayFormatter("myplay_coccmsea") {

  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }


  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDsFromHive: DataFrame = null
    var logoutDsFromHiv: DataFrame = null
    if (hourly == "") {
      val loginQuery = s"select * from coccmsea.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from coccmsea.logout where ds ='$logDate'"
      logoutDsFromHiv = getHiveLog(logoutQuery)
    }
    val loginDs = loginDsFromHive.selectExpr("log_date as log_date", "user_id as id", "level as level", "'0' as online_time")
    val logoutDs = logoutDsFromHiv.selectExpr("log_date as log_date", "user_id as id", "level as level", "online_time as online_time")
    var logDs = loginDs.union(logoutDs)
    logDs = logDs.withColumn("game_code", lit("myplay_coccmsea"))
    logDs

  }
}
