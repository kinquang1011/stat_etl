package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 08/02/2017.
  */
class Myplay_Bidacard extends MyplayFormatter ("myplay_bidacard"){
  import sqlContext.implicits._
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == ""){
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_bidacard*"
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
      ("myplay_bidacard", dateTime,id,gross_amt, net_amt)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs
  }
  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logRaw : RDD[String] = null
    if(hourly == ""){
      val logPatternPath = Constants.WAREHOUSE_DIR + "/myplay_bidacard/user/[yyyy-MM-dd]/*"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath,logDate)
      logRaw = getRawLog(logPath)
    }
    val filter = (line: String) => {
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\t")).filter(line => filter(line(0))).map { line =>
      val datetime = line(0)
      val userId = line (1)
      ("myplay_bidacard",datetime, userId)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    logDs
  }

}
