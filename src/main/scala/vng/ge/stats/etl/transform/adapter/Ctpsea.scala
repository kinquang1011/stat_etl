package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter


/**
  * Created by lamnt6 on 21/04/2017
  */
class Ctpsea extends FairyFormatter ("ctpsea") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    setWarehouseDir(Constants.WAREHOUSE_DIR)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var paymentDsFromHive: DataFrame = null
    if (hourly == "") {
      val paymentQuery = s"select * from ctpsea.recharge where ds ='$logDate'"
      paymentDsFromHive = getHiveLog(paymentQuery)
    }

    val sf = Constants.FIELD_NAME
    val _gamecode= gameCode


    var paymentDs = paymentDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "user_name as "+sf.ID, "server_id as "+sf.SID,"gross_revenue as "+sf.GROSS_AMT,
      "net_revenue as "+sf.NET_AMT,"level as "+sf.LEVEL, "transaction_id as "+sf.TRANS_ID,"ip_paying as "+sf.IP, "payingtype_id as "+sf.PAY_CHANNEL, "user_name as "+sf.RID
      )
    paymentDs = paymentDs.withColumn("game_code", lit(_gamecode))
    paymentDs=paymentDs.where("result = '1' ")
    paymentDs

  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var loginDsFromHive: DataFrame = null
    var logoutDsFromHive: DataFrame = null
    if (hourly == "") {
      val loginQuery = s"select * from ctpsea.login where ds ='$logDate'"
      loginDsFromHive = getHiveLog(loginQuery)
      val logoutQuery = s"select * from ctpsea.logout where ds ='$logDate'"
      logoutDsFromHive = getHiveLog(logoutQuery)
    }
    val getOs = udf { (osname: String) => {
      var os = "other"
      if(osname.toLowerCase().startsWith("android")){
        os = "android"
      }else if (osname.toLowerCase.startsWith("ios")){
        os = "ios"
      }
      os
    }
    }

    val sf = Constants.FIELD_NAME
    val _gamecode= gameCode


    val loginDs = loginDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "user_name as "+sf.ID, "server_id as "+sf.SID,
      "user_name as "+sf.RID,"device_name as "+sf.DEVICE,"os_platform as "+sf.OS,"os_version as "+sf.OS_VERSION,"'login' as "+sf.ACTION, "level as"+sf.LEVEL)

    val logoutDs = logoutDsFromHive.selectExpr("log_date as "+sf.LOG_DATE, "user_name as "+sf.ID, "server_id as "+sf.SID,
      "user_name as "+sf.RID,"device_name as "+sf.DEVICE,"os_platform as "+sf.OS,"os_version as "+sf.OS_VERSION,"'logout' as "+sf.ACTION, "level as"+sf.LEVEL)

    var ds = loginDs.union(logoutDs)
    ds = ds.withColumn(sf.GAME_CODE, lit(_gamecode))
    ds = ds.withColumn(sf.OS, getOs(col(sf.OS)))
    ds
  }
}
