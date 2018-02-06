package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.utils.{Common, DateTimeUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

import scala.util.Try
import scala.util.matching.Regex

/**
  * Created by lamnt6 on 30/10/2017.
  */
//contact point : T
//alpha test: 14/11/2017


class Farm extends FairyFormatter("farm") {

  import org.apache.spark.sql.functions.lit
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs: DataFrame = emptyDataFrame
    var sdkLoginRaw: RDD[String] = null
    var sdkApiRaw: RDD[String] = null

    val sf = Constants.FIELD_NAME

    val convertDate = udf { (timeStamp: Long) => {
      DateTimeUtils.getDate(timeStamp*1000)
    }
    }

    if (hourly == "") {
      val patternlLogin = Constants.GAMELOG_DIR + "/farmh5/[yyyyMMdd]/logs/*/player_login_[yyyyMMdd]*"

      var loginPath: Array[String] = null

      if(logDate.contains("2017-12-20")) {
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate,1)
      }else{
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      }

      loginDs = getCsvWithHeaderLog(loginPath, "\t")


      val patternlLoginSdk = Constants.GAMELOG_DIR + "/farmh5/[yyyyMMdd]/sdklog/LOGIN_nongtraih5*"
      val LoginSDKPath = PathUtils.generateLogPathDaily(patternlLoginSdk, logDate)
      sdkLoginRaw = getRawLog(LoginSDKPath)

      val patternlApi = Constants.GAMELOG_DIR + "/farmh5/[yyyyMMdd]/sdklog/API_nongtraih5_*"
      val ApiSDKPath = PathUtils.generateLogPathDaily(patternlApi, logDate)
      sdkApiRaw = getRawLog(ApiSDKPath)
    }

    loginDs = loginDs.withColumnRenamed("charId", sf.ID)
      .withColumn(sf.RID, col(sf.ID))
      .withColumn(sf.LOG_DATE, convertDate(col("time")))
      .withColumn(sf.SID, lit("1"))
      .withColumn(sf.ACTION, lit("login"))
      .withColumn(sf.GAME_CODE,lit("farm"))
    loginDs = loginDs.where("log_date like '"+logDate+"%'")
    loginDs=loginDs.drop("time")
    loginDs=loginDs.dropDuplicates()

    val filterLoginSdk = (line:Array[String]) => {
      var rs = false
      if ((line(3)).contains(logDate)) {
        rs = true
      }
      rs
    }

    val sdkLoginDs = sdkLoginRaw.map(line => line.split(",")).filter(line => filterLoginSdk(line)).map { line =>
      val idmapSdk = line(2)
      var channel = ""
      Try {
        val pattern = new Regex("utm_source=[A-Za-z0-9]+&")
        channel = (pattern findAllIn line(4)).mkString(",")
        if (!channel.equalsIgnoreCase("")) {
          channel = channel.replaceAll("utm_source=", "").replaceAll("&", "")
          if(channel == "0"){
            channel = "organic"
          }
        }else{
          channel= "organic"
        }
      }

      (idmapSdk,channel)
    }
      .toDF("idmapSdk",sf.CHANNEL)

    val filterApiSdk = (line:Array[String]) => {
      var rs = false
      if ((line(7)).contains(logDate)) {
        rs = true
      }
      rs
    }

    val sdkApiDs = sdkApiRaw.map(line => line.split(",")).filter(line => filterApiSdk(line)).map { line =>
      val idLogin = line(1)
      val idmapSdk = line(4)
      val guestChannel = line(3)
      (idmapSdk,idLogin,guestChannel)
    }
      .toDF("idmapSdk","idLogin","guestChannel")

    val channelParse = udf { (act: String, guestChannel:String) => {
      var result = act
      if(act== null || act.trim.isEmpty || act == "0"){
        if(guestChannel!=null && guestChannel.equalsIgnoreCase("CreateGuestRole")){
          result="guest"
        }else{
          result="organic"
        }
      }
      result
    }
    }

    val joinA = sdkApiDs.as('a).join(sdkLoginDs.as('b), sdkApiDs("idmapSdk") === sdkLoginDs("idmapSdk"), "left_outer").select("idLogin",sf.CHANNEL,"guestChannel")

    var join= loginDs.as("c").join(joinA.as("ja"), loginDs("id")===sdkApiDs("idLogin"), "left_outer")


    join = join.select(sf.ID,sf.RID,sf.SID,sf.GAME_CODE,sf.ACTION,sf.LOG_DATE,sf.CHANNEL,"guestChannel")

    join = join.withColumn(sf.CHANNEL,channelParse(col(sf.CHANNEL),col("guestChannel")))

    join

  }


  //  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
  //    var loginDs: DataFrame = emptyDataFrame
  //
  //    val sf = Constants.FIELD_NAME
  //
  //    val convertDate = udf { (timeStamp: Long) => {
  //      DateTimeUtils.getDate(timeStamp*1000)
  //    }
  //    }
  //
  //    if (hourly == "") {
  //      val patternlLogin = Constants.GAMELOG_DIR + "/farmh5/[yyyyMMdd]/logs/*/player_login_[yyyyMMdd]*"
  //
  //      var loginPath: Array[String] = null
  //
  //      if(logDate.contains("2017-12-20")) {
  //        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate,1)
  //      }else{
  //        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
  //      }
  //
  //      loginDs = getCsvWithHeaderLog(loginPath, "\t")
  //
  //
  //    }
  //
  //
  //    loginDs = loginDs.withColumnRenamed("charId", sf.ID)
  //      .withColumn(sf.RID, col(sf.ID))
  //      .withColumn(sf.LOG_DATE, convertDate(col("time")))
  //      .withColumn(sf.SID, lit("1"))
  //      .withColumn(sf.ACTION, lit("login"))
  //      .withColumn(sf.GAME_CODE,lit("farm"))
  //    loginDs = loginDs.where("log_date like '"+logDate+"%'")
  //    loginDs=loginDs.drop("time")
  //    loginDs=loginDs.dropDuplicates()
  //
  //    loginDs
  //  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentDs: DataFrame = emptyDataFrame
    val convertDate = udf { (timeStamp: Long) => {
      DateTimeUtils.getDate(timeStamp*1000)
    }
    }

    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/farmh5/[yyyyMMdd]/logs/*/player_recharge_[yyyyMMdd]*"
      var paymentPath: Array[String] = null

      if(logDate.contains("2017-12-20")) {
        paymentPath = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        paymentPath = PathUtils.generateLogPathDaily(pattern, logDate)
      }

      paymentDs = getCsvWithHeaderLog(paymentPath, "\\t")

    }

    paymentDs=paymentDs.select("charId","time","rmb","id")
    val sf = Constants.FIELD_NAME
    paymentDs = paymentDs.withColumnRenamed("id", sf.TRANS_ID)
      .withColumnRenamed("charId", sf.ID)
      .withColumn(sf.RID, col(sf.ID))
      .withColumn(sf.LOG_DATE, convertDate(col("time")))
      .withColumnRenamed("rmb",sf.GROSS_AMT)
      .withColumn(sf.NET_AMT,col(sf.GROSS_AMT))
      .withColumn(sf.GAME_CODE,lit("farm"))
      .withColumn(sf.SID, lit("1"))


    paymentDs = paymentDs.where("log_date like '"+logDate+"%'")
    paymentDs=paymentDs.drop("time")

    paymentDs

  }
}
