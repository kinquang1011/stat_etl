package vng.ge.stats.etl.transform.adapter


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter}
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
/**
  * Created by lamnt6 on 21/04/2017
  */
class Tnuh extends FairyFormatter ("tnuh") {
  import sqlContext.implicits._

  // 1 ngoc = 250 vnd (1 jade = 250vnd)
  val convertRate = 250

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }
//  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
//    import sparkSession.implicits._
//    val pattern = Constants.GAMELOG_DIR + "/tnuh/[yyyyMMdd]/datalog/l10vn-*"
//    val path = PathUtils.generateLogPathDaily(pattern, logDate,1)
//    val rawNewReg = getRawLog(path)
//
//    val filterlog = (line:String) => {
//      var rs = false
//      if(line.length>0 && line.contains(",")){
//        val str = line.substring(0,line.indexOf(",")).toLowerCase
//        if (str.contains(logDate) && str.contains("createrole")){
//          rs = true
//        }
//      }
//      rs
//    }
//
//    val sf = Constants.FIELD_NAME
//
//    val getOs = (device:String) => {
//      var os = "other"
//      if (device.toLowerCase.contains("ad")){
//        os = "android"
//      }else if (device.toLowerCase.contains("ios")){
//        os = "ios"
//      }
//      os
//    }
//
//    val registerLog = rawNewReg.filter(line => filterlog(line)).map { line =>
//      val str = line.substring(line.indexOf(",")+1,line.length)
//      val mapper = new ObjectMapper() with ScalaObjectMapper
//      mapper.registerModule(DefaultScalaModule)
//      val obj = mapper.readValue[Map[String, Object]](str)
//      val datetime =line.substring(line.indexOf("[")+1,line.indexOf("]"))
//      val ip= obj("ip").toString
//      val os = getOs(obj("os_name").toString)
//      val sid = obj("server").toString
//      val rid = obj("role_id").toString
//      val id = obj("account_id").toString
//      val roleName = obj("role_name").toString
//      ("tnuh", datetime, id, sid, rid, roleName, os, ip, "logout")
//    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.IP, sf.ACTION)
//
//    registerLog
//
//  }


  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    import sparkSession.implicits._
    val pattern = Constants.GAMELOG_DIR + "/tnuh/[yyyyMMdd]/datalog/l10vn-*"
    var path: Array[String] = null
    if(logDate.contains("2017-07-17")) {
      path = PathUtils.generateLogPathDaily(pattern, logDate,1)
    }else{
      path = PathUtils.generateLogPathDaily(pattern, logDate)
    }
    val raw = getRawLog(path)


    val filterlog = (line:String) => {
      var rs = false
      if(line.length>0 && line.contains(",")){
        val str = line.substring(0,line.indexOf(",")).toLowerCase
        if (str.contains(logDate) && (str.contains("prepaid") )){
          rs = true
        }
      }
      rs
    }

    val _converRate = convertRate
    val sf = Constants.FIELD_NAME

    val convertOs = Map(
      "ad" -> "android",
      "itouch" -> "ios",
      "ipad" -> "ios",
      "iphone" -> "ios"
    )
    val getOs = (device:String) => {
      var os = "other"
      if (convertOs.contains(device.toLowerCase)){
        os = convertOs(device.toLowerCase)
      }
      os
    }


    val payment = raw.filter(line => filterlog(line)).map { line =>
      val str = line.substring(line.indexOf(",")+1,line.length)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val obj = mapper.readValue[Map[String, Object]](str)
      val datetime =line.substring(line.indexOf("[")+1,line.indexOf("]"))
      val sid = obj("server").toString
      val id = obj("account_id").toString
      val gross = (obj("jade").toString.toDouble*_converRate).toString
      val net = (obj("jade").toString.toDouble*_converRate).toString
      val rid =  obj("account_id").toString
      val transactionId =  obj("sn").toString
      val os =  getOs(obj("device_type").toString)

//      val index = str.indexOf("transaction_id")
//      var transactionId=""
//      if(index>0){
//        val sub = str.substring(index,str.indexOf(",",index))
//        val array = sub.split(":")
//        if(array.length>0){
//          transactionId=array(1).replace("\"","")
//        }
//      }


      ("tnuh", datetime, id, rid, sid,transactionId,gross,net,os)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT,sf.OS)


    payment

  }



//  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
//    import sparkSession.implicits._
//    val pattern = Constants.GAMELOG_DIR + "/tnuh/[yyyyMMdd]/datalog/l10vn-*"
//    val path = PathUtils.generateLogPathDaily(pattern, logDate)
//    val raw = getRawLog(path)
//
//
//    val filterlog = (line:String) => {
//      var rs = false
//      if(line.length>0 && line.contains(",")){
//        val str = line.substring(0,line.indexOf(",")).toLowerCase
//        if (str.contains(logDate) && (str.contains("onlinerolenum") )){
//          rs = true
//        }
//      }
//      rs
//    }
//
//    val sf = Constants.FIELD_NAME
//
//    val loginRawLog = raw.filter(line => filterlog(line)).map { line =>
//      val str = line.substring(line.indexOf(",")+1,line.length)
//      val mapper = new ObjectMapper() with ScalaObjectMapper
//      mapper.registerModule(DefaultScalaModule)
//      val obj = mapper.readValue[Map[String, Object]](str)
//      val datetime =line.substring(line.indexOf("[")+1,line.indexOf("]"))
//      val ccu= obj("online").toString
//      val sid = obj("server").toString
//      ("tnuh", datetime, ccu, sid)
//    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.CCU, sf.SID)
//
//
//    loginRawLog
//
//  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/tnuh/[yyyyMMdd]/datalog/l10vn-*"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      raw = getRawLog(loginPath)
    }else {
      //      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      //      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      //      loginRaw = getRawLog(loginPath)
    }

    val getDateTime = (timestamp:String) => {
      var rs = timestamp
      if(timestamp!=null){
        rs = DateTimeUtils.getDate(timestamp.toLong)
      }
      rs
    }
    val filterlogin = (line:String) => {
      var rs = false
      if(line.length>0 && line.contains(",") ){
        val str = line.substring(0,line.indexOf(",")).toLowerCase
        if (str.contains(logDate) && str.contains("login")){
          rs = true
        }
      }
      rs
    }

    val filterlogout = (line:String) => {
      var rs = false
      if(line.length>0 && line.contains(",") ){
        val str = line.substring(0,line.indexOf(",")).toLowerCase
        if (str.contains(logDate) && str.contains("logout")){
          rs = true
        }
      }
      rs
    }

    val sf = Constants.FIELD_NAME

    val getOs = (device:String) => {
      var os = "other"
      if (device.toLowerCase.contains("ad")){
        os = "android"
      }else if (device.toLowerCase.contains("ios")){
        os = "ios"
      }
      os
    }

    val loginRawLog = raw.filter(line => filterlogin(line)).map { line =>
      val str = line.substring(line.indexOf(",")+1,line.length)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val obj = mapper.readValue[Map[String, Object]](str)
      val datetime =line.substring(line.indexOf("[")+1,line.indexOf("]"))
      val ip= obj("ip").toString
      val scrW=obj("device_width").toString
      val scrH=obj("device_height").toString
      val os = getOs(obj("os_name").toString)
//      val os = obj("os_name").toString

      val sid = obj("server").toString
      val rid = obj("account_id").toString
      val level = obj("role_level").toString
      val resolution = scrW+"," + scrH
      val id = obj("account_id").toString
      val roleName = obj("role_name").toString
      ("tnuh", datetime, id, sid, rid, roleName, os, level,resolution, ip, "login")
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.LEVEL, sf.RESOLUTION, sf.IP, sf.ACTION)

//    val logoutRawLog = raw.filter(line => filterlogout(line)).map { line =>
//      val str = line.substring(line.indexOf(",")+1,line.length)
//      val mapper = new ObjectMapper() with ScalaObjectMapper
//      mapper.registerModule(DefaultScalaModule)
//      val obj = mapper.readValue[Map[String, Object]](str)
//      val datetime =line.substring(line.indexOf("[")+1,line.indexOf("]"))
//      val ip= obj("ip").toString
//      val scrW=obj("device_width").toString
//      val scrH=obj("device_height").toString
//      val os = getOs(obj("os_name").toString)
////      val os = obj("os_name").toString
//
//      val sid = obj("server").toString
//      val rid = obj("account_id").toString
//      val level = obj("role_level").toString
//      val resolution = scrW+"," + scrH
//      val id = obj("account_id").toString
//      val roleName = obj("role_name").toString
//      ("tnuh", datetime, id, sid, rid, roleName, os, level,resolution, ip, "logout")
//    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.LEVEL, sf.RESOLUTION, sf.IP, sf.ACTION)
//
//    val df = loginRawLog.union(logoutRawLog)

    loginRawLog
  }
}










