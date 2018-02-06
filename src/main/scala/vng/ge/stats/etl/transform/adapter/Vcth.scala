package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 21/04/2017
  */
class Vcth extends FairyFormatter ("vcth") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    setWarehouseDir(Constants.WAREHOUSE_DIR)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.WAREHOUSE_DIR + "/vcth/login/[yyyy-MM-dd]/*"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.WAREHOUSE_DIR + "/vcth/logout/[yyyy-MM-dd]/*"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
      logoutRaw = getRawLog(logoutPath)
    }

    val filterlog = (line: Array[String]) => {
      var rs = false
      if (line.length>7 && line(0).startsWith(logDate)){
        rs = true
      }
      rs
    }

//
//    def loginGenerate(arr: Array[String]): Row = {
//      Row(gameCode, arr(0), "login", arr(1), arr(3), arr(7), arr(6), arr(5))
//    }
//
//    def logoutGenerate(arr: Array[String]): Row = {
//      Row(gameCode, arr(0), "logout", arr(1), arr(3), arr(7), arr(6), arr(5))
//    }
//
//    var sf = Constants.LOGIN_LOGOUT_FIELD
//    var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.IP, sf.LEVEL)
//    var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.IP, sf.LEVEL)

    val sf = Constants.FIELD_NAME

    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val ip = line(6)
      val roleId = line(3)
      val sid = line(7)
      val level = line(5)
      val action = "login"
      ("vcth",dateTime,id,ip,roleId,sid,level,action)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.IP,sf.RID,sf.SID,sf.LEVEL,sf.ACTION)



    val logoutDs = logoutRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val ip = line(6)
      val roleId = line(3)
      val sid = line(7)
      val level = line(5)
      val action = "login"
      ("vcth",dateTime,id,ip,roleId,sid,level,action)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.IP,sf.RID,sf.SID,sf.LEVEL,sf.ACTION)

    val ds: DataFrame = loginDs.union(logoutDs)
    ds
  }
}
