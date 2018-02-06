package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 08/02/2017.
  */
class Myplay_Superfarm extends MyplayFormatter("myplay_superfarm") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var loginDsFromHive: DataFrame = null
    var logoutDsFromHive: DataFrame = null
    if (hourly == "") {
      val loginQuery = s"select * from superfarm.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from superfarm.logout where ds ='$logDate'"
      logoutDsFromHive = getHiveLog(logoutQuery)
    }
    var loginDs = loginDsFromHive.selectExpr("log_date as log_date", "account_name as id", "server_id as sid",
      "role_id as rid ")
    loginDs = loginDs.withColumn("online_time", lit(0))
    loginDs = loginDs.withColumn("action", lit("login"))
    var logoutDs = logoutDsFromHive.selectExpr("log_date as log_date", "account_name as id", "server_id as sid",
      "role_id as rid ", "online_time as online_time")
    logoutDs = logoutDs.withColumn("action", lit("logout"))
    var ds = loginDs.union(logoutDs)
    ds = ds.withColumn("game_code", lit("myplay_superfarm"))
    ds
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val registerPattern = Constants.WAREHOUSE_DIR + "/superfarm/register/[yyyy-MM-dd]/*"
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
      ("myplay_superfarm", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }
}
