package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 21/04/2017
  */
class CubeEtl extends FairyFormatter ("cffbs") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val payingPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/IFRS/IFRS.gz"
      val payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate)
      paymentRaw = getRawLog(payingPath)
    }else {
      val payingPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/IFRS/IFRS_[yyyyMMdd]_*"
      val payingPath = PathUtils.generateLogPathHourly(payingPattern, logDate)
      paymentRaw = getRawLog(payingPath)
    }


    val getDateTime = (timestamp:String) => {
      var rs = timestamp
      if(timestamp!=null){
        rs = DateTimeUtils.getDate(timestamp.toLong*1000)
      }
      rs
    }

    val filterLog = (line: Array[String]) =>{
      var rs = false
      if (getDateTime(line(3)).startsWith(logDate) && line(10).toLong>0 && line(11).toLong>0){
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split(",")).filter(line => filterLog(line)).map { line =>
      val id = line(0)
      val dateTime = getDateTime(line(3))
      val serverId = "1"
      val gross = line(10)
      val net = line(11)
      val transactionId = line(13)
//      val roleId = line(14)
      ("cffbs", dateTime,id,serverId,transactionId,gross,net)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs


  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN.gz"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGOUT/LOGOUT.gz"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
      logoutRaw = getRawLog(logoutPath)
    }else {
      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGOUT/LOGOUT-[yyyy-MM-dd]_*"
      val logoutPath = PathUtils.generateLogPathHourly(logoutPattern, logDate)
      logoutRaw = getRawLog(logoutPath)
    }

    val filterlog = (line:String) => {
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val getOs = (s: String) => {
      var rs = "other"
      if (s!=null && !s.isEmpty) {
        rs = s.toLowerCase()
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line(0))).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val serverId = "1"
      val roleId = line(3)
      val roleName = line(4)
      val os =getOs(line(6))
      val osVersion = line(7)
      val deviceId = line(8)
      val device = line(9)
      val level = line(11)
      val action = "login"
      ("cffbs", dateTime,id,serverId,roleId,roleName,os,osVersion,deviceId,device,level,action)
    }
    .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.OS_VERSION,sf.DID,sf.DEVICE,sf.LEVEL,sf.ACTION)


    val logoutDs = logoutRaw.map(line => line.split("\\t")).filter(line => filterlog(line(0))).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val serverId = "1"
      val roleId = line(3)
      val roleName = line(4)
      val os = getOs(line(6))
      val osVersion = line(7)
      val deviceId = line(8)
      val device = line(9)
      val level = line(11)
      val action = "logout"
      ("cffbs", dateTime,id,serverId,roleId,roleName,os,osVersion,deviceId,device,level,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.OS_VERSION,sf.DID,sf.DEVICE,sf.LEVEL,sf.ACTION)
    val ds: DataFrame = loginDs.union(logoutDs)
    ds
  }
}
