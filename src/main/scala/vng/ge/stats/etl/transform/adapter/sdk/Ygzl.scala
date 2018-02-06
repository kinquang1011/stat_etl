package vng.ge.stats.etl.transform.adapter.sdk

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.SdkFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt on 15/11/2017
  */
class Ygzl extends SdkFormatter ("ygzl") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var registerRaw: RDD[String] = null
    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/[yyyyMMdd]/login_log/zl/app_users.txt"



//    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/login_log/full/zl/app_users.txt"

    val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate,1)
    registerRaw = getRawLog(registerPath)
    val sf = Constants.FIELD_NAME


    val filter = (line:Array[String]) => {
      var rs = false
      if (line.length>26 && (line(10)).contains(logDate)){
        rs = true
      }
      rs
    }

    val registerDf = registerRaw.map(line => line.replaceAll("[{].*.[}]", "").split(",")).filter(line => filter(line)).map { line =>
      val date = line(10).replace("\"","")
      val id = line(2).replace("\"","")
      var channel = line(26).replace("\"","")

      if(channel == null || channel.isEmpty) {
        channel="direct"
      }

      ("ygzl", date, id, channel)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.CHANNEL)

    registerDf
  }
  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    //todo ==> file path 2 ngay lientiep, xong dung func val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
    var dailyLogin: DataFrame = null
    val sf = Constants.FIELD_NAME
    var logPath: Array[String] = null

    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/ygh5/[yyyyMMdd]/login_log/zl/log_user_login.txt"
//      val logPattern = Constants.GAME_LOG_DIR + "/ygh5/login_log/full/zl/log_user_login.txt"

      if(logDate.contains("2018-01-15")) {
        logPath = PathUtils.generateLogPathDaily(logPattern, logDate,1)
      }else{
        logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      }

      dailyLogin = getCsvLog(logPath,",")
    } else {


    }

    dailyLogin=dailyLogin.select("_c2","_c3")
    .withColumnRenamed("_c2",sf.ID)
    .withColumnRenamed("_c3",sf.LOG_DATE)
    .withColumn(sf.GAME_CODE, lit("ygzl"))

      .where("log_date like'"+logDate+"%'")

    var registerRaw: RDD[String] = null
    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/[yyyyMMdd]/login_log/zl/app_users.txt"

//    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/login_log/full/zl/app_users.txt"

    val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate,1)
    registerRaw = getRawLog(registerPath)

    val filter = (line:Array[String]) => {
      var rs = false
      if (line.length>26){
        rs = true
      }
      rs
    }

    val registerDf = registerRaw.map(line => line.replaceAll("[{].*.[}]", "").split(",")).filter(line => filter(line)).map { line =>
      val id = line(2).replace("\"","")
      var channel = line(26).replace("\"","")

      if(channel == null || channel.isEmpty) {
        channel="direct"
      }

      (id, channel)
    }.toDF(sf.ID, sf.CHANNEL)

    var join = dailyLogin.as('a).join(registerDf.as('b), dailyLogin(sf.ID) === registerDf(sf.ID), "left_outer").where("log_date like'"+logDate+"%'")
    join=join.select("a.id",sf.LOG_DATE,sf.CHANNEL,sf.GAME_CODE)
    join
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {

    val setAmtWithRate = udf { (amt: String, changeRate: Double) => {
      var vnd: Double = 0
      vnd = amt.toDouble * changeRate

      vnd.toString
    }
    }
    val sf = Constants.FIELD_NAME

    var payment: DataFrame = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/sdk/[yyyy-MM-dd]/Log_YUGIOHZLH5_DBGAdd/" + "Log_YUGIOHZLH5_DBGAdd-[yyyy-MM-dd].gz"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      payment = getJsonLog(logPath)


    } else {
//      val logPattern = Constants.GAME_LOG_DIR + "/" + sdkSource + "/[yyyy-MM-dd]/Log_" + sdkGameCode + "_DBGAdd/"
//      val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
//      payment = getJsonLog(logPath)

    }
    if (payment.rdd.isEmpty()) {
      payment = createEmptyPaymentDs()
    } else {
      payment = payment.withColumn(sf.TRANS_ID, col("transactionID"))
      payment = payment.withColumn(sf.ID, col("userID"))
      payment = payment.withColumn(sf.RID, col("userID"))
      payment = payment.withColumn(sf.GAME_CODE, lit("ygzl"))
      payment = payment.withColumn(sf.NET_AMT, setAmtWithRate(col("pmcNetChargeAmt"), lit(1)))
      payment = payment.withColumn(sf.GROSS_AMT, setAmtWithRate(col("pmcGrossChargeAmt"), lit(1)))
      payment = payment.withColumn(sf.LOG_DATE, col("updatetime"))
      payment = payment.where("log_date like '"+logDate+"%'").select(sf.LOG_DATE, sf.ID, sf.TRANS_ID, sf.NET_AMT, sf.GROSS_AMT, sf.GAME_CODE, sf.RID)

    }
    payment
//
//    var registerRaw: RDD[String] = null
//    //    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygzl/[yyyyMMdd]/login_log/vn/app_users.txt"
//
//    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygzl/login_log/full/vn/app_users.txt"
//
//    val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate,1)
//    registerRaw = getRawLog(registerPath)
//
//    val filter = (line:Array[String]) => {
//      var rs = false
//      if (line.length>26){
//        rs = true
//      }
//      rs
//    }
//
//    val registerDf = registerRaw.map(line => line.replaceAll("[{].*.[}]", "").split(",")).filter(line => filter(line)).map { line =>
//      val id = line(2).replace("\"","")
//      var channel = line(26).replace("\"","")
//
//      if(channel == null || channel.isEmpty) {
//        channel="direct"
//      }
//
//      (id, channel)
//    }.toDF(sf.ID, sf.CHANNEL)
//
//    var join = payment.as('a).join(registerDf.as('b), payment(sf.ID) === registerDf(sf.ID), "left_outer").where("log_date like'"+logDate+"%'")
//    join=join.select(sf.LOG_DATE, "a.id", sf.TRANS_ID, sf.NET_AMT, sf.GROSS_AMT, sf.GAME_CODE, sf.RID,sf.CHANNEL)
//    join


  }
}
