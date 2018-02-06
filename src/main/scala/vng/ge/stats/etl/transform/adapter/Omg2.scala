package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, PathUtils}

/**
  * Created by quangctn on 03/07/2017
  */
//contact point : dungnvb
// hdfs dfs -text /ge/gamelogs/omg2/20170806/datalog/game/*/stat.log.* | awk -F"|" '{print $1}' | sort -u
//Alpha Test dont have recharge, ob have
class Omg2 extends FairyFormatter("omg2") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logRaw: RDD[String] = null
    if (hourly == "") {
      /* val patternUserPath = Constants.GAMELOG_DIR + "/omg2/logtest/stat.log"*/
      val patternUserPath = Constants.GAMELOG_DIR + "/omg2/[yyyyMMdd]/datalog/game/*/stat.log.*"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
      logRaw = getRawLog(userPath)
    }
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length > 5) {
        if ((MyUdf.timestampToDate((line(5)).toLong * 1000).startsWith(logDate)) &&
          (line(0).contains("RoleLogin") || line(0).contains("RoleLogout"))) {
          rs = true
        }
      }
        rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "1") {
        platform = "ios"
      } else if (s == "2") {
        platform = "android"
      }
      platform
    }

    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\|")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(5).toLong * 1000)
      val sid = line(1)
      val id = line(2)
      val os = getOs(line(3))
      val rid = line(4)
      var ip = ""
      var did = ""
      var level = ""
      var action = ""
      if (line(0).contains("RoleLogin")) {
        ip = line(6)
        did = line(7)
        action = "login"
      } else if (line(0).contains("RoleLogout") && line.length >= 10) {

        ip = line(7)
        did = line(8)
        level = line(10)
        action = "logout"
      }
      ("omg2", datetime, sid, id, os, rid, ip, did, level, action)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ID, sf.OS, sf.RID, sf.IP, sf.DID, sf.LEVEL, sf.ACTION)
    logDs
  }
  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var logRaw: RDD[String] = null
    if (hourly == "") {
      val patternPayPath = Constants.GAME_LOG_DIR + "/omg2/[yyyyMMdd]/datalog/web/*/stat.log.*"
      val payPath = PathUtils.generateLogPathDaily(patternPayPath, logDate)
      logRaw = getRawLog(payPath)
    } else {
      //Hourly
    }
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length >= 17) {
        if ((MyUdf.timestampToDate((line(5)).toLong * 1000).startsWith(logDate)) &&
          //Log Charge
            (line(0).contains("Charge")) &&
          //Status 4 = success
          (line(7).equalsIgnoreCase("4"))) {
          rs = true
        }
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "1") {
        platform = "ios"
      } else if (s == "2") {
        platform = "android"
      }
      platform
    }

    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\|")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(line(5).toLong * 1000)
      val sid = line(1)
      val id = line(2)
      val os = getOs(line(3))
      val rid = line(4)
      val ip = line(12)
      val trans_id = line(9)
      val gross_amt = line(11).toLong * 250
      val net_amt = gross_amt
      val did = line(13)
      val channel = line(16)

      ("omg2", datetime, sid, id, os, rid, ip, trans_id,gross_amt ,net_amt,did,channel )
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ID, sf.OS, sf.RID, sf.IP,sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT, sf.DID, sf.CHANNEL)
    logDs
  }
}
