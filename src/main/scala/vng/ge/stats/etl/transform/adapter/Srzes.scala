package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 17/10/2017.
  * Contact point : silp
  * release: 25/10/2017
  */
class Srzes extends FairyFormatter ("srzes") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var registerRaw: RDD[String] = null

    val patternlLoginRegister = Constants.GAMELOG_DIR + "/srzes/[yyyy-MM-dd]/user_register*"
    val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate)
    registerRaw = getRawLog(registerPath)


    val filter = (line:Array[String]) => {
      var rs = false
      if (line.length>=13 && (line(0)).startsWith(logDate) && line(10) == "1") {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val registerDs = registerRaw.map(line => line.split(",")).filter(line => filter(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val roleId = line(1)
      val roleName = line(1)
      val sid = line(5)
      val os = line(12)

      ("srzes", dateTime,id,roleId,roleName,sid,os)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.OS)

    registerDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/srzes/[yyyy-MM-dd]/user_paying*"
      var path: Array[String] = null
      if(logDate.contains("2017-12-28")) {
        path = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        path = PathUtils.generateLogPathDaily(pattern, logDate)
      }
      paymentRaw = getRawLog(path)
    }else{

    }

    val filter = (line:Array[String]) => {
      var rs = false
      if (line.length>=22 && (line(0)).startsWith(logDate) && line(15) == "1") {
        rs = true
      }
      rs
    }

//    val filter = (line:Array[String]) => {
//      var rs = false
//      if (line.length>=22 && line(15) == "1") {
//        rs = true
//      }
//      rs
//    }

    val getOs = (flatform:String) => {
      var os = "other"
      if (flatform.equals("ios")){
        os = "ios"
      }else if (flatform.equals("android")){
        os = "android"
      }
      os
    }

    val setAmtWithRate =  (amt: String) => {
      var vnd: Double = 0
      vnd = amt.toDouble/100
      vnd.toString
    }


    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split(",")).filter(line => filter(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val roleId = line(1)
      val roleName = line(1)
      val sid = line(18)
      val os = getOs(line(21))
      val gross = line(6)
      val net = line(6)
      val tranId =line(4)

      ("srzes", dateTime,id,roleId,roleName,sid,os,gross,net,tranId)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.OS,sf.GROSS_AMT,sf.NET_AMT,sf.TRANS_ID)

    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var registerRaw: RDD[String] = null

    if (hourly == "") {
      val patternLogin = Constants.GAMELOG_DIR + "/srzes/[yyyy-MM-dd]/user_login*"
      var pathLogin: Array[String] = null
      if(logDate.contains("2017-07-17")) {
        pathLogin = PathUtils.generateLogPathDaily(patternLogin, logDate,1)
      }else{
        pathLogin = PathUtils.generateLogPathDaily(patternLogin, logDate)
      }
      loginRaw = getRawLog(pathLogin)

      val patternlLoginRegister = Constants.GAMELOG_DIR + "/srzes/[yyyy-MM-dd]/user_register*"
      val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate)
      registerRaw = getRawLog(registerPath)

    }else{

    }
    val filterlogin = (line:Array[String]) => {
      var rs = false
      if (line.length>=16 && (line(0)).startsWith(logDate) && line(13) == "1") {
        rs = true
      }
      rs
    }

//    val filterlogin = (line:Array[String]) => {
//      var rs = false
//      if (line.length>=16 && line(13) == "1") {
//        rs = true
//      }
//      rs
//    }

    val getOs = (flatform:String) => {
      var os = "other"
      if (flatform.equals("ios")){
        os = "ios"
      }else if (flatform.equals("android")){
        os = "android"
      }
      os
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split(",")).filter(line => filterlogin(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val roleId = line(1)
      val roleName = line(1)
      val sid = line(5)
      val os = getOs(line(15))
      val action = "login"

      ("srzes", dateTime,id,roleId,roleName,sid,os,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.OS,sf.ACTION)

    val filterRegister = (line:Array[String]) => {
      var rs = false
      if (line.length>=14 && (line(0)).startsWith(logDate) && line(10) == "1") {
        rs = true
      }
      rs
    }

    val registerDs = registerRaw.map(line => line.split(",")).filter(line => filterRegister(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val roleId = line(1)
      val roleName = line(1)
      val sid = line(5)
      val os = line(12)
      val action = "login"

      ("srzes", dateTime,id,roleId,roleName,sid,os,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.OS,sf.ACTION)

    val ds = loginDs.union(registerDs)

    ds
  }

}
