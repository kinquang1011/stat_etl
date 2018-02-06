package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 30/10/2017.
  */
//contact point : Datlc
//alpha test: 14/11/2017


class Vtl extends FairyFormatter("vtl") {

  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs: DataFrame = null
    var logoutDs: DataFrame = null

    val getDateTime = udf { (timestamp: String) => {
      var rs = timestamp
      if(timestamp!=null){
        rs = DateTimeUtils.getDate(timestamp.toLong*1000)
      }
      rs
    }
    }


    if (hourly == "") {
      val patternlLogin = Constants.GAMELOG_DIR + "/vtl/[yyyy-MM-dd]/datalog/*_logout_user_[yyyy-MM-dd].log"

      var loginPath: Array[String] = null
      var logoutPath: Array[String] = null

      if(logDate.contains("2017-12-19")) {
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate,1)
      }else{
        loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      }
      loginDs = getJsonLog(loginPath).select("accountName","roleId","roleName","serverId","level","time","ip")

      val patternlLogout = Constants.GAMELOG_DIR + "/vtl/[yyyy-MM-dd]/datalog/*_logout_user_[yyyy-MM-dd].log"
      if(logDate.contains("2017-12-19")) {
        logoutPath = PathUtils.generateLogPathDaily(patternlLogout, logDate,1)
      }else{
        logoutPath = PathUtils.generateLogPathDaily(patternlLogout, logDate)
      }
      logoutDs = getJsonLog(logoutPath).select("accountName","roleId","roleName","serverId","level","time","ip")
    }
    val sf = Constants.FIELD_NAME
    loginDs = loginDs.withColumnRenamed("serverId", sf.SID)
      .withColumnRenamed("accountName", sf.ID)
      .withColumnRenamed("roleId", sf.RID)
      .withColumnRenamed("roleName", sf.ROLE_NAME)
      .withColumnRenamed("ip", sf.IP)
      .withColumnRenamed("level", sf.LEVEL)
      .withColumn(sf.LOG_DATE,getDateTime(col("time")))
      .withColumn(sf.ACTION,lit("login"))
      .withColumn(sf.GAME_CODE,lit("vtl"))
    loginDs=loginDs.drop("time").where("log_date like '"+logDate+"%'")

    logoutDs = logoutDs.withColumnRenamed("serverId", sf.SID)
      .withColumnRenamed("accountName", sf.ID)
      .withColumnRenamed("roleId", sf.RID)
      .withColumnRenamed("roleName", sf.ROLE_NAME)
      .withColumnRenamed("ip", sf.IP)
      .withColumnRenamed("level", sf.LEVEL)
      .withColumn(sf.LOG_DATE,getDateTime(col("time")))
      .withColumn(sf.ACTION,lit("logout"))
      .withColumn(sf.GAME_CODE,lit("vtl"))
    logoutDs=logoutDs.drop("time").where("log_date like '"+logDate+"%'")

    loginDs.union(logoutDs);

  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var paymentDs: DataFrame = emptyDataFrame

    val getDateTime = udf { (timestamp: String) => {
      var rs = timestamp
      if(timestamp!=null){
        rs = DateTimeUtils.getDate(timestamp.toLong*1000)
      }
      rs
    }
    }

    val setAmtWithRate = udf { (amt: String) => {
      var vnd: Double = 0
      vnd = amt.toDouble * 100
      vnd.toString
    }
    }

    if (hourly == "") {
      var paymentPath: Array[String] = null

      val pattern = Constants.GAMELOG_DIR + "/vtl/[yyyy-MM-dd]/datalog/*normal_recharge_[yyyy-MM-dd].log"
      if(logDate.contains("2018-01-04")) {
        paymentPath = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        paymentPath = PathUtils.generateLogPathDaily(pattern, logDate)
      }
      paymentDs = getJsonLog(paymentPath)
    }

    paymentDs=paymentDs.select("accountName","roleId","roleName","serverId","addCoin","transactionId","time")

    paymentDs = paymentDs.withColumnRenamed("serverId", sf.SID)
      .withColumnRenamed("accountName", sf.ID)
      .withColumnRenamed("roleId", sf.RID)
      .withColumnRenamed("roleName", sf.ROLE_NAME)
      .withColumn(sf.LOG_DATE,getDateTime(col("time")))
      .withColumnRenamed("transactionId", sf.TRANS_ID)
      .withColumn(sf.GAME_CODE,lit("vtl"))

    paymentDs=paymentDs.withColumn(sf.GROSS_AMT,setAmtWithRate(col("addCoin")))
    paymentDs=paymentDs.withColumn(sf.NET_AMT,setAmtWithRate(col("addCoin")))
    paymentDs=paymentDs.drop("addCoin","time").where("log_date like '"+logDate+"%'")


    paymentDs

  }
}
