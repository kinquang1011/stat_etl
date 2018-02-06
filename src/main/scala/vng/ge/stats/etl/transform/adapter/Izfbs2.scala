package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 21/04/2017
  */
class Izfbs2 extends FairyFormatter ("izfbs2") {
  import sqlContext.implicits._
  val timeIncr:Long =  39600

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val _timeIncr= timeIncr
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val payingPattern = Constants.GAMELOG_DIR + "/izfbs2/[yyyy-MM-dd]/user_paying/user_paying-[yyyy-MM-dd]"
      var payingPath: Array[String] = null
      if(logDate.contains("2017-06-16")) {
        payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate,1)
      }else{
        payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate)
      }
      paymentRaw = getRawLog(payingPath)
    } else {

    }

    val _gamecode= gameCode

    val filterlog = (line: Array[String]) => {
      var rs = false
      if (line.length>=21 && MyUdf.dateTimeIncrement(line(1),_timeIncr).startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map {
      line =>
        val id = line(16)
        val rid = line(16)
        val dateTime = MyUdf.dateTimeIncrement(line(1),_timeIncr)
        val serverId = line(0)
        val gross = line(7)
        val net = line(7)
        val transactionId = line(4)
        val game_code =_gamecode
        (game_code, dateTime, id, serverId, transactionId, gross, net,rid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT,sf.RID)
    paymentDs

  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    val patternNewReg = Constants.GAMELOG_DIR + "/izfbs2/[yyyy-MM-dd]/user_register/user_register-[yyyy-MM-dd]"

    var pathNewReg: Array[String] = null

    if(logDate.contains("2017-06-16")) {
      pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate,1)
    }else{
      pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate)
    }
    val rawNewReg = getRawLog(pathNewReg)

    val _timeIncr= timeIncr
    val _gamecode= gameCode

    val newRegFilter = (line:Array[String]) => {
      var rs = false
      if (line.length>=9 &&  MyUdf.dateTimeIncrement(line(1),_timeIncr).startsWith(logDate) && !line(2).isEmpty && line(6)=="1") {
        rs = true
      }
      rs

    }

    val sf = Constants.FIELD_NAME
    val getOs = (device:String) => {
      var os = "other"
      if (device.toLowerCase.contains("android")){
        os = "android"
      }else if (device.toLowerCase.contains("ios")){
        os = "ios"
      }
      os
    }

    val newRegDf = rawNewReg.map(line => line.split("\\t")).filter(line => newRegFilter(line)).map { line =>
      val date = MyUdf.dateTimeIncrement(line(1),_timeIncr)
      val sid = line(0)
      val id = line(2)
      val rid = line(2)
      val roleName = line(3)
      val ip = line(4)
      val did = line(7)
      val os = getOs(line(8))
      val game_code =_gamecode
      (game_code,date,sid,id,rid,roleName,ip,did,os)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID,sf.ID,sf.RID,sf.ROLE_NAME,sf.IP,sf.DID,sf.OS)


    newRegDf
  }


  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    val _timeIncr= timeIncr
    val _gamecode= gameCode

    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/izfbs2/[yyyy-MM-dd]/user_logout/user_logout-[yyyy-MM-dd]"

      var loginPath: Array[String] = null
      if(logDate.contains("2017-06-16")) {
        loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate,1)
      }else{
        loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      }
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.GAMELOG_DIR + "/izfbs2/[yyyy-MM-dd]/user_login/user_login-[yyyy-MM-dd]"

      var logoutPath: Array[String] = null
      if(logDate.contains("2017-06-16")) {
        logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate,1)
      }else{
        logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
      }
      logoutRaw = getRawLog(logoutPath)
    }else {

    }
    val getOs = (device:String) => {
      var os = "other"
      if (device.toLowerCase.contains("android")){
        os = "android"
      }else if (device.toLowerCase.contains("ios")){
        os = "ios"
      }
      os
    }

    val filterlog = (line:Array[String]) => {
      var rs = false
      if (line.length>=13 && MyUdf.dateTimeIncrement(line(1),_timeIncr).startsWith(logDate)) {
        rs = true
      }
      rs
    }



    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = MyUdf.dateTimeIncrement(line(1),_timeIncr)
      val id = line(2)
      val serverId = line(0)
      val roleId = line(2)
      val roleName = line(3)
      val ip = line(4)
      val os =getOs(line(13))
      val deviceId = line(11)
      val level = line(6)
      val action = "login"
      val game_code =_gamecode

      (game_code, dateTime,id,serverId,roleId,roleName,os,ip,deviceId,level,action)
    }
    .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.IP,sf.DID,sf.LEVEL,sf.ACTION)

    val logoutDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = MyUdf.dateTimeIncrement(line(1),_timeIncr)
      val id = line(2)
      val serverId = line(0)
      val roleId = line(2)
      val roleName = line(3)
      val ip = line(4)
      val os =getOs(line(13))

      val deviceId = line(11)
      val level = line(6)
      val action = "logout"
      val game_code =_gamecode

      (game_code, dateTime,id,serverId,roleId,roleName,os,ip,deviceId,level,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.IP,sf.DID,sf.LEVEL,sf.ACTION)

    val ds: DataFrame = loginDs.union(logoutDs)
    ds
  }
}
