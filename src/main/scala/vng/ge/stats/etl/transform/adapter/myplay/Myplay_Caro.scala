package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 08/02/2017.
  */
class Myplay_Caro extends MyplayFormatter ("myplay_caro"){
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == ""){
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_caro*"
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
      val money = line(4)
      val id = line(2)
      ("myplay_caro", dateTime,id,money, money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginLogoutRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/carobz/[yyyy-MM-dd]/carobz_user/*"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      loginLogoutRaw = getRawLog(logPath)
    }

    val filterLoginLogout = (line: String) => {
      var rs = false
      if (MyUdf.timestampToDate(line.toLong).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).filter(line => filterLoginLogout(line(0))).map { line =>
      val dateTime = MyUdf.timestampToDate(line(0).toLong)
      val id = line(1)
      val action = "login"
      ("myplay_caro", dateTime, action, id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID)
    loginLogoutDs
  }
}
