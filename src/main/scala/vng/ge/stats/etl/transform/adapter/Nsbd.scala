package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils


/**
  * Created by lamnt6 on 30/10/2017.
  */
//contact point : LuanTq



class Nsbd extends FairyFormatter("nsbd") {

  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs: DataFrame = null
    if (hourly == "") {
      val patternlLogin = Constants.GAMELOG_DIR + "/nsbd_mobile/[yyyy-MM-dd]/datalog/*/accountLogin.log"
      var loginPath: Array[String] = null
      if (logDate.contains("2017-07-17")) {
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate, 1)
      } else {
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      }
      loginDs = getJsonLog(loginPath)

    }
    loginDs= loginDs.select("c_t","detail.account._id.$oid","detail.device.pf","detail.account.channel","serverId")

    val sf = Constants.FIELD_NAME

    val getOs = udf { (os: String) => {
      var result = "other"
      if(os.toLowerCase.contains("iphone") || os.toLowerCase.contains("ios")){
        result="ios"
      }else if(os.toLowerCase.contains("android")){
        result="android"
      }
      result
    }
    }


    loginDs = loginDs.withColumnRenamed("$oid", sf.ID)
      .withColumnRenamed("serverId", sf.SID)
      .withColumn(sf.RID, col(sf.ID))
      .withColumnRenamed("c_t", sf.LOG_DATE)
      .withColumn(sf.GAME_CODE,lit("nsbd"))
      .withColumn(sf.ACTION,lit("login"))
      .withColumn(sf.OS,getOs(col("pf")))
      .withColumn(sf.CHANNEL,getOs(col("channel")))
    loginDs=loginDs.where("log_date like '"+logDate+"%'")
    loginDs=loginDs.select(sf.ID,sf.RID,sf.SID,sf.LOG_DATE,sf.ACTION,sf.GAME_CODE,sf.OS,sf.CHANNEL)

    loginDs
  }


  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var paymentDs: DataFrame = emptyDataFrame
    if (hourly == "") {
      val patternlLogin = Constants.GAMELOG_DIR + "/nsbd_mobile/[yyyy-MM-dd]/datalog/*/pay.log"
      var paymentPath: Array[String] = null
      if (logDate.contains("2017-07-17")) {
        paymentPath = PathUtils.generateLogPathDaily(patternlLogin, logDate,1)
      } else {
        paymentPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      }

      paymentDs = getJsonLog(paymentPath)

    }

    val getOs = udf { (os: String) => {
      var result = "other"
      if(os.toLowerCase.contains("iphone") || os.toLowerCase.contains("ios")){
        result="ios"
      }else if(os.toLowerCase.contains("android")){
        result="android"
      }
      result
    }
    }

    if(!paymentDs.rdd.isEmpty() && paymentDs!=emptyDataFrame){
      paymentDs= paymentDs.select("detail.playerPay.cp_id","c_t","detail.playerPay.fee","detail.playerPay.plat","detail.playerPay.channel","serverId","detail.playerPay._id.$oid")
      paymentDs = paymentDs.withColumn(sf.ID,col("$oid"))
        .withColumnRenamed("serverId", sf.SID)
        .withColumn(sf.RID, col(sf.ID))
        .withColumnRenamed("c_t", sf.LOG_DATE)
        .withColumnRenamed("channel", sf.CHANNEL)
        .withColumnRenamed("cp_id", sf.TRANS_ID)
        .withColumn(sf.GAME_CODE,lit("nsbd"))
        .withColumn(sf.NET_AMT, col("fee"))
        .withColumn(sf.GROSS_AMT, col("fee"))
        .withColumn(sf.OS,getOs(col("plat")))
      paymentDs=paymentDs.where("log_date like '"+logDate+"%'")
      paymentDs = paymentDs.select(sf.ID,sf.SID,sf.RID,sf.LOG_DATE,sf.CHANNEL,sf.TRANS_ID,sf.GAME_CODE,sf.GROSS_AMT,sf.NET_AMT,sf.OS)

    }
    paymentDs

  }

}
