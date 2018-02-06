package vng.ge.stats.etl.transform.adapter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 21/04/2017
  */
class Dtm extends FairyFormatter ("dtm") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    import sparkSession.implicits._
    val pattern = Constants.GAMELOG_DIR + "/dtm/[yyyyMMdd]/datalog/gamelog_[yyyyMMdd]/*/*/*.log"
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

    val payment = raw.filter(line => filterlog(line)).map { line =>
      val str = line.substring(line.indexOf(",")+1,line.length)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val obj = mapper.readValue[Map[String, Object]](str)
      val datetime =line.substring(line.indexOf("[")+1,line.indexOf("]"))
      val sid = obj("server").toString
      val id = obj("account_id").toString
      val gross = obj("cash").toString
      val net = obj("cash").toString
      val rid =  obj("role_id").toString
      val transactionId =  obj("sn").toString
//      val os = getOs(obj("os_name").toString)

      ("dtm", datetime, id, rid, sid,transactionId,gross,net)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT)


    payment

  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/dtm/[yyyyMMdd]/datalog/gamelog_[yyyyMMdd]/*/*/*.log"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
    raw = getRawLog(loginPath)
    }else {
      //      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      //      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      //      loginRaw = getRawLog(loginPath)
    }

    val filterlog = (line:String) => {
      var rs = false
      if(line.length>0 && line.contains(",") ){
        val str = line.substring(0,line.indexOf(",")).toLowerCase
        if (str.contains(logDate) && (str.contains("loginrole") || str.contains("logoutrole"))){
          rs = true
        }
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

    val loginRawLog = raw.filter(line => filterlog(line)).map { line =>
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
      val rid = obj("role_id").toString
      val level = obj("role_level").toString
      val resolution = scrW+"," + scrH
      val id = obj("account_id").toString
      val roleName = obj("role_name").toString

      var action="login"
      if(line.substring(0,line.indexOf(",")).toLowerCase.contains("logoutrole")){
        action="logout"
      }

      ("dtm", datetime, id, sid, rid, roleName, os, level,resolution, ip, action)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.LEVEL, sf.RESOLUTION, sf.IP, sf.ACTION)


    loginRawLog
  }
}










