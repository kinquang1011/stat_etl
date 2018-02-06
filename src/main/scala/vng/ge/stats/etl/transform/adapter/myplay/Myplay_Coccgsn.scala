package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 08/02/2017.
  */
class Myplay_Coccgsn extends MyplayFormatter("myplay_coccgsn") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.WAREHOUSE_DIR + "/coccgsn/login/[yyyy-MM-dd]/*.gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
    }
    val logFilter = (arr: Array[String]) => {
      var rs = false
      if (arr.length >= 5 && arr(0).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => logFilter(line)).map { line =>
      val dateTime = line(0)
      val id = line(3)
      ("myplay_coccgsn", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    loginDs
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val registerPattern = Constants.WAREHOUSE_DIR + "/coccm/register/[yyyy-MM-dd]/*"
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
      ("myplay_coccgsn", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }

}
