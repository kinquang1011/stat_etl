package vng.ge.stats.etl.transform.adapter.pc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{DbgGameFormatter, FairyFormatter}
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 23/02/2017.
  */
//contact point : trungn2
//hdfs dfs -cat /ge/gamelogs/tvc/20170504/*/t_user_pay* |wc-l
class Tvc extends DbgGameFormatter("tvc") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logUserRaw: RDD[String] = null
    var logRaw: RDD[String] = null
    if (hourly == "") {
      val patternUserPath = Constants.GAMELOG_DIR + "/tvc/[yyyyMMdd]/*/t_users_[0-9]*.csv"
      val userPath = PathUtils.generateLogPathDaily(patternUserPath, logDate)
      logUserRaw = getRawLog(userPath)
      val logPatternPath = Constants.GAMELOG_DIR + "/tvc/[yyyyMMdd]/*/t_log_in_out_*"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate)
      logRaw = getRawLog(logPath)
    }

    val trimQuote = (s: String) => {
      s.replace("\"", "")
    }

    val filterLog = (line: Array[String]) => {
      var rs = false
      if (MyUdf.timestampToDate(trimQuote(line(1)).toLong * 1000).startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split(",")).filter(line => filterLog(line)).map { line =>
      val datetime = MyUdf.timestampToDate(trimQuote(line(1)).toLong * 1000)
      val onlineTime = trimQuote(line(3))
      val roleId = trimQuote(line(0))
      val sid = trimQuote(line(5))
      ("tvc", datetime, onlineTime, roleId, sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ONLINE_TIME, sf.RID, sf.SID)
    val logUserDs = logUserRaw.map(line => line.split(",")).filter(l => l.length >= 8).map { line =>
      val roleId = trimQuote(line(0))
      val account = trimQuote(line(1))
      val roleName = trimQuote(line(2))
      val level = trimQuote(line(3))
      val ip = trimQuote(line(5))
      val sid = trimQuote(line(7))
      (roleId, account, roleName, level, ip, sid)
    }.toDF(sf.RID, sf.ID, sf.ROLE_NAME, sf.LEVEL, sf.IP, sf.SID)
    var join = logDs.as('a).join(logUserDs.as('b),
      logDs("rid") === logUserDs("rid"), "left_outer")
    join = join.drop(col("b.rid")).drop(col("b.sid"))
    join
  }

}
