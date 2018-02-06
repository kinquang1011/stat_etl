package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 12/12/2017
  */
class Gbm extends FairyFormatter ("gbm") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    //    var logoutRaw: RDD[String] = null
    //    var logoutPath: Array[String] = null
    var loginPath: Array[String] = null

    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/gbm/[yyyy-MM-dd]/datalog/VNGLog/login_*.log"
      if(logDate.contains("2018-01-02")) {
        loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate,1)
      }else{
        loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      }
      loginRaw = getRawLog(loginPath)

      //      val logoutPattern = Constants.GAMELOG_DIR + "/gbm/[yyyy-MM-dd]/datalog/VNGLog/logout_*.log"
      //      if(logDate.contains("2018-01-02")) {
      //        logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate,1)
      //      }else{
      //        logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
      //      }
      //
      //      logoutRaw = getRawLog(logoutPath)
    }else {

    }

    val filterlog = (line: Array[String]) => {
      var rs = false
      if (line.length>=10 && line(0).startsWith(logDate)){
        rs = true
      }
      rs
    }
    val filterlogout = (line: Array[String]) => {
      var rs = false
      if (line.length>=10 && line(0).startsWith(logDate)){
        rs = true
      }
      rs
    }

    val getOs = (s: String) => {
      var rs = "other"
      if (s.toLowerCase.equals("android")) {
        rs = "android"
      }else if (s.toLowerCase.equals("ios")) {
        rs = "ios"
      }
      rs
    }



    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(0)
      val id = line(2)
      val serverId = line(4)
      val channel = line(9)
      val roleId = id
      val roleName = id
      val os =getOs(line(8))
      val action = "login"
      ("gbm", dateTime,id,serverId,roleId,roleName,os,action,channel)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.ACTION,sf.CHANNEL)

    //    val logoutDs = logoutRaw.map(line => line.split("\t")).filter(line => filterlog(line)).map { line =>
    //      val dateTime = line(0)
    //      val id = line(2)
    //      val serverId = line(4)
    //      val roleId = id
    //      val roleName = id
    //      val os =getOs(line(9))
    //      val action = "logout"
    //      ("gbm", dateTime,id,serverId,roleId,roleName,os,action)
    //    }
    //      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.ACTION)
    loginDs
    //    val ds: DataFrame = loginDs.union(logoutDs)
    //    ds
  }
}
