package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.Formatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by canhtq on 26/06/2017.
  */

class GunPow extends Formatter("ddd2mp2"){

  import sqlContext.implicits._
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if(!hourly.isEmpty){
      val paymentPattern = Constants.GAME_LOG_DIR + "/ddd2mp2/[yyyy-MM-dd]/datalog/hours/[yyyyMMdd]/dandandao2-*"
      val paymentPath = PathUtils.generateLogPathHourly(paymentPattern, logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    val paymentFilter = (line: Array[String]) => {
      var rs = false
      if (line.length==16 && line.apply(15).equalsIgnoreCase("CHARGE_LOG") && line.apply(12)=="1") {
        rs = true
      }
      rs
    }
    val sc = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\$\\$")).filter(line => paymentFilter(line)).map { line =>
      val netRev = line(7).toDouble
      val dateTime = line(6)
      val sid = line(11)
      val rid = line(2)
      val id = line(0)
      ("ddd2mp2", dateTime, id, rid, sid, netRev,netRev)
    }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.RID, sc.SID, sc.NET_AMT, sc.GROSS_AMT)
    paymentDs.show(false)
    paymentDs
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginLogoutRaw: RDD[String] = null
    if(!hourly.isEmpty){
      val pattern = Constants.GAME_LOG_DIR + "/ddd2mp2/[yyyy-MM-dd]/datalog/hours/[yyyyMMdd]/dandandao2-*"
      val path = PathUtils.generateLogPathHourly(pattern, logDate)
      loginLogoutRaw = getRawLog(path)
    }

    val mFilter = (line: Array[String]) => {
      var rs = false
      if (line.length==13 && line.apply(12).equalsIgnoreCase("ROLE_LOGIN_LOG")) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\$\\$")).filter(line => mFilter(line)).map { line =>
      val dateTime = line(6)
      val sid = line(7)
      val rid = line(1)
      val id = line(0)
      ("ddd2mp2", dateTime, "login", id, rid, sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID)
    loginLogoutDs
  }
}
