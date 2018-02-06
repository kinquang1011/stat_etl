package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter}
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 15/06/2017
  */
class Sglfbs extends FairyFormatter ("sglfbs") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val payingPattern = Constants.GAMELOG_DIR + "/sglfbs/[yyyy-MM-dd]/paying/paying-[yyyy-MM-dd]"
      var payingPath: Array[String] = null
      if(logDate.contains("2017-07-27")){
         payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate,1)
      }else{
         payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate)
      }
      paymentRaw = getRawLog(payingPath)
    }else {

    }


    val filterlog = (line:Array[String]) => {
      var rs = false
      if ((line(0)).startsWith(logDate) && line(7)>"0" && line(15)=="0"){
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line=>filterlog(line)).map{
      line =>
        val id = line(1)
        val dateTime = line(0)
        val serverId = line(18)
        val gross = line(6)
        val net = line(7)
        val transactionId = line(4)
        val roleId = line(16)
        ("sglfbs", dateTime,id,serverId,transactionId,gross,net,roleId)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT,sf.RID)
    paymentDs



  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    import sparkSession.implicits._
    val patternNewReg = Constants.GAMELOG_DIR + "/sglfbs/[yyyy-MM-dd]/register/register-[yyyy-MM-dd]"
    val pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate, 1)
    val rawNewReg = getRawLog(pathNewReg)
    val filterLog = (line: Array[String]) =>{
      var rs = false
      if (line(0).contains(logDate)){
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

    val newRegDf = rawNewReg.map(line => line.split("\\t")).filter(line => filterLog(line)).map { line =>
      val date = line(0)
      val sid = line(4)
      val id = line(1)
      val os = getOs(line(12))

      ("sglfbs",date,id,sid,os)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.OS)


    newRegDf
  }
//
//  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
//    var paymentRaw: RDD[String] = null
//    if (hourly == "") {
//      val payingPattern = Constants.GAMELOG_DIR + "/sglfbs/cgmtfbs/cgmtfbs/paying/paying-[yyyy-MM-dd]_00000"
//      val payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate)
//      paymentRaw = getRawLog(payingPath)
//
//    }else {
//
//    }
//
//    val filterLog = (line: Array[String]) =>{
//      var rs = false
//      if (line(0).contains(logDate)){
//        rs = true
//      }
//      rs
//    }
//    val sf = Constants.FIELD_NAME
//    val paymentDf = paymentRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map { line =>
//      val id = line(1)
//      val dateTime = line(0)
//      val transactionId = line(4)
//      val gross = line(7)
//      val net = line(7)
//      val roleId = line(7)
//      val roleName = line(7)
//      val sid = line(18)
//      ("sglfbs", dateTime,id,gross,net,sid,transactionId,roleId,roleName)
//    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.GROSS_AMT, sf.NET_AMT, sf.SID,sf.TRANS_ID,sf.RID,sf.ROLE_NAME)
//    paymentDf
//
//
//  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null

    if (hourly == "") {

      val loginPattern = Constants.GAMELOG_DIR + "/sglfbs/[yyyy-MM-dd]/login/login-[yyyy-MM-dd]"
      var loginPath: Array[String] = null

      if(logDate.contains("2017-07-27")) {
        loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate,1)

      }else{
        loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      }
      loginRaw = getRawLog(loginPath)

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
      if ((line(0)).startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val roleId = line(2)
      val roleName = line(3)
      val sid = line(5)
      val level = line(6)
      val os = getOs(line(14))
      val action = "login"
      ("sglfbs", dateTime,id,roleId,roleName,sid,level,os,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.LEVEL,sf.OS,sf.ACTION)

    loginDs

  }
}
