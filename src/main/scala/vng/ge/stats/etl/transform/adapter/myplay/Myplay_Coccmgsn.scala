package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 08/02/2017.
  */
class Myplay_Coccmgsn extends MyplayFormatter("myplay_coccmgsn") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.WAREHOUSE_DIR + "/coccm/login/[yyyy-MM-dd]/*.gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.WAREHOUSE_DIR + "/coccm/logout/[yyyy-MM-dd]/*.gz"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
      logoutRaw = getRawLog(logoutPath)
    } else {
      //hourly
    }
    val logFilter = (arr: Array[String]) => {
      var rs = false
      if (arr.length >= 36 && arr(0).startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => logFilter(line)).map { line =>
      val dateTime = line(0)
      val id = line(3)
      val action = "login"
      ("myplay_coccmgsn", dateTime, id,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE,sf.ID,sf.ACTION)
    val logoutDs = logoutRaw.map(line => line.split("\\t")).filter(line => logFilter(line)).map { line =>
      val dateTime = line(0)
      val id = line(3)
      val action = "logout"
      ("myplay_coccmgsn", dateTime, id,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE,sf.ID, sf.ACTION)
    val ds: DataFrame = loginDs.union(logoutDs)
    ds
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val registerPattern = Constants.WAREHOUSE_DIR + "/coccgsn/register/[yyyy-MM-dd]/*"
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
      val id = line(3)
      val dateTime = line(0)
      ("myplay_coccmgsn", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }
}
