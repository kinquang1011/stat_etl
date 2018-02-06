package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 17/10/2017.
  * Contact point : silp
  * release: 25/10/2017
  */
class Dmzth extends FairyFormatter ("dmzth") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }


  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/dmztl/[yyyyMMdd]/loggame/logserver/*/tab_pay_[yyyyMMdd]*.log"
      var path: Array[String] = null
      if(logDate.contains("2018-01-16")) {
        path = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        path = PathUtils.generateLogPathDaily(pattern, logDate)
      }
      paymentRaw = getRawLog(path)
    }else{
      val pattern = Constants.GAMELOG_DIR + "/dmztl/[yyyyMMdd]/loggame/logserver/*/tab_pay_[yyyyMMdd]*.log"
      var path: Array[String] = null
      path = PathUtils.generateLogPathDaily(pattern, logDate,1)
      paymentRaw = getRawLog(path)
    }
    val filter = (line:Array[String]) => {
      var rs = false
      if (line.length>14 && (line(1)).startsWith(logDate)){
        rs = true
      }
      rs
    }

    val getOs = (flatform:String) => {
      var os = "other"
      if (flatform.equals("1")){
        os = "ios"
      }else if (flatform.equals("2")){
        os = "android"
      }
      os
    }
    val getChannel = (idChannel:String) => {
      var channel = "other"

      val convertChannel = Map(
        "1086" -> "VNG_ZMI",
        "1087" -> "VNG_ZM",
        "1088" -> "VNG_FBI",
        "1089" -> "VNG_FB",
        "1090" -> "VNG_ZLI",
        "1091" -> "VNG_ZL",
        "1092" -> "VNG_GGI",
        "1093" -> "VNG_GG"
      )
      if (convertChannel.contains(idChannel.toLowerCase)){
        channel = convertChannel(idChannel.toLowerCase)
      }
      channel
    }
    val setAmtWithRate =  (amt: String) => {
      // game nay 1 bath = 700 Vnd confirm anh Longtb
      val convertRate = 700

      var vnd: Double = 0
      vnd = (amt.toDouble/100)*convertRate
      vnd.toString
    }


    val sf = Constants.FIELD_NAME
    var paymentDs=emptyDataFrame
    if(paymentRaw != null){
      paymentDs = paymentRaw.map(line => line.split(",")).filter(line => filter(line)).map { line =>
        val dateTime = line(1)
        val id = line(4)
        val roleId = line(5)
        val roleName = line(3)
        val sid = line(0)
        val os = getOs(line(6))
        val channel = getChannel(line(2))
        val gross = setAmtWithRate(line(13))
        val net = setAmtWithRate(line(13))

        ("dmzth", dateTime,id,roleId,roleName,sid,os,channel,gross,net)
      }
        .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.OS,sf.CHANNEL,sf.GROSS_AMT,sf.NET_AMT)

    }

    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val patternLogin = Constants.GAMELOG_DIR + "/dmztl/[yyyyMMdd]/" +
        "loggame/logserver/*/tab_login_[yyyyMMdd]*.log"
      val patternLogout = Constants.GAMELOG_DIR + "/dmztl/[yyyyMMdd]/" +
        "loggame/logserver/*/tab_logout_[yyyyMMdd]*.log"
      val pathLogin = PathUtils.generateLogPathDaily(patternLogin, logDate)
      val pathLogout = PathUtils.generateLogPathDaily(patternLogout, logDate)
      loginRaw = getRawLog(pathLogin)
      logoutRaw = getRawLog(pathLogout)
    }else{
      val patternLogin = Constants.GAMELOG_DIR + "/dmztl/[yyyyMMdd]/" +
        "loggame/logserver/*/tab_login_[yyyyMMdd]*.log"
      val patternLogout = Constants.GAMELOG_DIR + "/dmztl/[yyyyMMdd]/" +
        "loggame/logserver/*/tab_logout_[yyyyMMdd]*.log"
      val pathLogin = PathUtils.generateLogPathDaily(patternLogin, logDate,1)
      val pathLogout = PathUtils.generateLogPathDaily(patternLogout, logDate,1)
      loginRaw = getRawLog(pathLogin)
      logoutRaw = getRawLog(pathLogout)
    }
    val filterlogin = (line:Array[String]) => {
      var rs = false
      if (line.length>8 && (line(1)).startsWith(logDate)){
        rs = true
      }
      rs
    }

    val filterlogout = (line:Array[String]) => {
      var rs = false
      if (line.length>12 && (line(1)).startsWith(logDate)){
        rs = true
      }
      rs
    }

    val getOs = (flatform:String) => {
      var os = "other"
      if (flatform.equals("1")){
        os = "ios"
      }else if (flatform.equals("2")){
        os = "android"
      }
      os
    }
    val getChannel = (idChannel:String) => {
      var channel = "other"

      val convertChannel = Map(
        "1086" -> "VNG_ZMI",
        "1087" -> "VNG_ZM",
        "1088" -> "VNG_FBI",
        "1089" -> "VNG_FB",
        "1090" -> "VNG_ZLI",
        "1091" -> "VNG_ZL",
        "1092" -> "VNG_GGI",
        "1093" -> "VNG_GG"
      )
      if (convertChannel.contains(idChannel.toLowerCase)){
        channel = convertChannel(idChannel.toLowerCase)
      }
      channel
    }


    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split(",")).filter(line => filterlogin(line)).map { line =>
      val dateTime = line(1)
      val id = line(4)
      val roleId = line(5)
      val roleName = line(3)
      val sid = line(0)
      val level = line(8)
      val os = getOs(line(6))
      val channel = getChannel(line(2))
      val action = "login"

      ("dmzth", dateTime,id,roleId,roleName,sid,level,os,channel,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.LEVEL,sf.OS,sf.CHANNEL,sf.ACTION)

    val logoutDs = logoutRaw.map(line => line.split(",")).filter(line => filterlogout(line)).map { line =>
      val dateTime = line(1)
      val id = line(4)
      val roleId = line(5)
      val roleName = line(3)
      val sid = line(0)
      val level = line(12)
      val os = getOs(line(6))
      val channel = getChannel(line(2))
      val action = "logout"
      ("dmzth", dateTime,id,roleId,roleName,sid,level,os,channel,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.LEVEL,sf.OS,sf.CHANNEL,sf.ACTION)

    val ds = loginDs.union(logoutDs)
    ds
  }

}
