package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 30/10/2017.
  */
//contact point : datlc
//alpha test: 14/01/2018


class Skt extends FairyFormatter("skt") {

  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs: DataFrame = null
    if (hourly == "") {
      val patternlLogin = Constants.GAMELOG_DIR + "/skt/[yyyy-MM-dd]/datalog/*_user_login_*.log"
      var loginPath: Array[String] = null
      if(logDate.contains("2018-01-01")) {
         loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate,1)
      }else{
         loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      }
      loginDs = getJsonLog(loginPath)
    }else{
      val patternlLogin = Constants.GAMELOG_DIR + "/skt/[yyyy-MM-dd]/datalog/*_user_login_*.log"
      var loginPath: Array[String] = null
      if(logDate.contains("2018-01-01")) {
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate,1)
      }else{
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      }
      loginDs = getJsonLog(loginPath)
    }
    val sf = Constants.FIELD_NAME
    loginDs = loginDs.withColumnRenamed("serverid", sf.SID)
      .withColumnRenamed("account", sf.ID)
      .withColumn(sf.RID,col(sf.ID))
      .withColumnRenamed("ip", sf.IP)
      .withColumnRenamed("logintime", sf.LOG_DATE)
      .withColumn(sf.ACTION,lit("login"))
      .withColumn(sf.GAME_CODE,lit("skt"))

    loginDs=loginDs.where("log_date like '"+logDate+"%'");

    loginDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME

    val setAmtWithRate = udf { (amt: String) => {
      var vnd: Double = 0
      vnd = amt.toDouble * 100
      vnd.toString
    }
    }

    var paymentDs: DataFrame = emptyDataFrame
    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/skt/[yyyy-MM-dd]/datalog/*_recharge_*.log"

      var paymentPath: Array[String] = null
      if(logDate.contains("2018-01-01")) {
         paymentPath = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
         paymentPath = PathUtils.generateLogPathDaily(pattern, logDate)
      }
      paymentDs = getJsonLog(paymentPath)

      if(paymentDs != emptyDataFrame){
        paymentDs = paymentDs.withColumnRenamed("serverId", sf.SID)
          .withColumnRenamed("account", sf.ID)
          .withColumn(sf.RID,col(sf.ID))
          .withColumnRenamed("rechargeTime", sf.LOG_DATE)
          .withColumnRenamed("transId", sf.TRANS_ID)
          .withColumn(sf.GAME_CODE,lit("skt"))

        paymentDs=paymentDs.withColumn(sf.GROSS_AMT,setAmtWithRate(col("points")))
        paymentDs=paymentDs.withColumn(sf.NET_AMT,setAmtWithRate(col("points")))
        paymentDs=paymentDs.where("log_date like '"+logDate+"%'").dropDuplicates(sf.TRANS_ID);

      }
    }
    paymentDs

  }
}
