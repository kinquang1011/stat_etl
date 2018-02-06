package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.DateTimeUtils


/**
  * Created by lamnt6 on 21/04/2017
  */
class Nlmb extends FairyFormatter ("nlmb") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var paymentDsFromHive: DataFrame = null
    if (hourly == "") {
      val paymentQuery = s"select * from nlmb.recharge where ds ='$logDate'"
      paymentDsFromHive = getHiveLog(paymentQuery)
    }

    val sf = Constants.FIELD_NAME
    val _gamecode= gameCode


    var paymentDs = paymentDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "account_name as "+sf.ID, "server_id as "+sf.SID,"gold * 100 as "+sf.GROSS_AMT,
      "gold * 100 as "+sf.NET_AMT, "order_id as "+sf.TRANS_ID, "account_name as "+sf.RID
      )
    paymentDs = paymentDs.withColumn("game_code", lit(_gamecode))
    paymentDs

  }

//  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
//    import org.apache.spark.sql.functions.lit
//    var registerDsFromHive: DataFrame = null
//    val registerQuery = s"select * from nlmb.new_register where ds ='$logDate'"
//    registerDsFromHive = getHiveLog(registerQuery)
//
//    val sf = Constants.FIELD_NAME
//    val _gamecode= gameCode
//
//    var registerDs = registerDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "account_name as "+sf.ID, "server_id as "+sf.SID,
//      "role_id as "+sf.RID)
//
//    registerDs = registerDs.withColumn("game_code", lit(_gamecode))
//    registerDs
//  }


  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var loginDsFromHive: DataFrame = null
    var logoutDsFromHive: DataFrame = null
    if (hourly == "") {
      val loginQuery = s"select * from nlmb.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from nlmb.logout where ds ='$logDate'"
      logoutDsFromHive = getHiveLog(logoutQuery)
    }
    val getOs = udf { (sid: String) => {
      var os = ""
      if(sid.startsWith("9")){
        os = "android"
      }else if (sid.startsWith("6")){
        os = "ios"
      }
      os
    }
    }

    val sf = Constants.FIELD_NAME
    val _gamecode= gameCode


    val loginDs = loginDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "account_name as "+sf.ID, "server_id as "+sf.SID,
      "account_name as "+sf.RID,"'login' as "+sf.ACTION, "level as"+sf.LEVEL)

    val logoutDs = logoutDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "account_name as "+sf.ID, "server_id as "+sf.SID,
      "account_name as "+sf.RID,"'logout' as "+sf.ACTION, "level as"+sf.LEVEL)

    var ds = loginDs.union(logoutDs)
    ds = ds.withColumn(sf.GAME_CODE, lit(_gamecode))
    ds = ds.withColumn(sf.OS, getOs(col("sid")))
    ds
  }
}
