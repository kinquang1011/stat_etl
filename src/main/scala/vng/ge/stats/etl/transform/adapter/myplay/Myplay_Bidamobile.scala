package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils
/**
  * Created by quangctn on 09/02/2017.
  */
class Myplay_Bidamobile extends MyplayFormatter ("myplay_bidamobile"){
  import sqlContext.implicits._
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == ""){
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_bidamobile_vi*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath,logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    val filterLog = (line: String) =>{
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val gross_amt = line(4)
      val id = line(2)
      val net_amt = gross_amt.toDouble * line(9).toDouble
      ("myplay_bidamobile", dateTime,id,gross_amt, net_amt)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    var logoutRaw: RDD[String] = null
    if(hourly == ""){
      val loginPattern = Constants.WAREHOUSE_DIR + "/myplay_bidamobile/login/[yyyy-MM-dd]/*"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern,logDate)
          loginRaw = getRawLog(loginPath)
      val logoutPattern = Constants.WAREHOUSE_DIR + "/myplay_bidamobile/logout/[yyyy-MM-dd]/*"
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern,logDate)
      logoutRaw = getRawLog(logoutPath)
    }else{

    }
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length > 19) {
        if (line(0).startsWith(logDate) && line(19).startsWith("VN")) {
          rs = true
        }
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map{ line =>
      val datetime = line(0)
      val id = line(1)
      val sid = line(2)
      val rid = line (3)
      val rolename = line(4)
      val platform = line (6)
      val deviceName = line(8)
      val level = line(10)
      val did = line(18)
      val onlineTime = 0
      val action = "login"
      ("myplay_bidamobile",datetime, action ,id ,sid ,rid ,rolename ,platform ,deviceName ,level ,did, onlineTime)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID,sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.DEVICE, sf.LEVEL, sf.DID, sf.ONLINE_TIME)

    val logoutDs = logoutRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map{ line =>
      val datetime = line(0)
      val id = line(1)
      val sid = line(2)
      val rid = line (3)
      val rolename = line(4)
      val platform = line (6)
      val deviceName = line(8)
      val level = line(10)
      val did = ""
      val onlineTime = line(16)
      val action = "logout"
      ("myplay_bidamobile",datetime, action ,id ,sid ,rid ,rolename ,platform ,deviceName ,level,did, onlineTime)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID,sf.SID, sf.RID, sf.ROLE_NAME, sf.OS, sf.DEVICE, sf.LEVEL,sf.DID,sf.ONLINE_TIME)
    val ds: DataFrame = loginDs.union(logoutDs)
    ds
    }
   override def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var registerRaw: RDD[String] = null
    val registerPattern = Constants.WAREHOUSE_DIR + "/myplay_bidamobile/new_register/[yyyy-MM-dd]/*"
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
      val id = line(1)
      val dateTime = line(0)
      ("myplay_bidamobile", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }
}
