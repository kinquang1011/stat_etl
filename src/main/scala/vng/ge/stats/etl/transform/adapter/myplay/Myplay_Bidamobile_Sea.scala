package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 14/02/2017.
  */
class Myplay_Bidamobile_Sea extends MyplayFormatter ("myplay_bidamobile_sea"){
  import sqlContext.implicits._
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentBidaIdRaw: RDD[String] = null
    var paymentBidaInterRaw: RDD[String] = null
    var paymentBidaMyRaw: RDD[String] = null
    var paymentBidaThRaw: RDD[String] = null
    if (hourly == ""){
      val paymentBidaIdPattern = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_bidamobile_id*"
      val paymentBidaInterPattern = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_bidamobile_inter*"
      val paymentBidaMyPattern = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_bidamobile_my*"
      val paymentBidaThPattern = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_bidamobile_th*"
      val paymentBidaIdPath = PathUtils.generateLogPathDaily(paymentBidaIdPattern,logDate)
      val paymentBidaInterPath = PathUtils.generateLogPathDaily(paymentBidaInterPattern,logDate)
      val paymentBidaMyPath = PathUtils.generateLogPathDaily(paymentBidaMyPattern,logDate)
      val paymentBidaThPath = PathUtils.generateLogPathDaily(paymentBidaThPattern,logDate)
      paymentBidaIdRaw = getRawLog(paymentBidaIdPath)
      paymentBidaInterRaw = getRawLog(paymentBidaInterPath)
      paymentBidaMyRaw = getRawLog(paymentBidaMyPath)
      paymentBidaThRaw = getRawLog(paymentBidaThPath)
    }
    val filterLog = (line: String) => {
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentBidaIdDs = paymentBidaIdRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val money = line(4)
      val id = line(2)
      ("myplay_bidamobile_sea", dateTime,id,money, money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)

    val paymentBidaInterDs = paymentBidaInterRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val money = line(4)
      val id = line(2)
      ("myplay_bidamobile_sea", dateTime,id,money,money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT,sf.NET_AMT)

    val paymentBidaMyDs = paymentBidaMyRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val money = line(4)
      val id = line(2)
      ("myplay_bidamobile_sea", dateTime,id,money,money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT,sf.NET_AMT)

    val paymentBidaThDs = paymentBidaThRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val money = line(4)
      val id = line(2)
      ("myplay_bidamobile_sea", dateTime,id,money,money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT,sf.NET_AMT)
    val paymentDs = paymentBidaIdDs.union(paymentBidaInterDs.union(paymentBidaMyDs.union(paymentBidaThDs)))
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
      if(line.length>19){
        if (line(0).startsWith(logDate) && !line(19).startsWith("VN")) {
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
      ("myplay_bidamobile_sea",datetime, action ,id ,sid ,rid ,rolename ,platform ,deviceName ,level ,did, onlineTime)
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
      ("myplay_bidamobile_sea",datetime, action ,id ,sid ,rid ,rolename ,platform ,deviceName ,level,did, onlineTime)
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
    val filterLog = (line: Array[String]) => {
      var rs = false
      if(line(0).startsWith(logDate) && !line(11).startsWith("VN") ){
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val registerDs = registerRaw.map(line => line.split("\\t")).filter(line => filterLog(line)).map { line =>
      val id = line(1)
      val dateTime = line(0)
      ("myplay_bidamobile_sea", dateTime, id)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    registerDs
  }
}
