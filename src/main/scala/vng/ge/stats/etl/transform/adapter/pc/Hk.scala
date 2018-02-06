package vng.ge.stats.etl.transform.adapter.pc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, input_file_name, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

/**
  * Created by quangctn on 23/02/2017.
  */
//contact point : trungn2
class Hk extends DbgGameFormatter("hk") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  private val convertToDateTime = udf { (datetime: String) => {
    var time =""
    if(datetime!=null && datetime.matches("\\d+")){
      time = MyUdf.timestampToDate(datetime.toLong * 1000)
    }
    time

  }
  }
  private val calcTotalOnline = udf { (loginTime: String, logoutTime: String) => {
    var totalOnline = 0
    if(loginTime.matches("\\d+") && logoutTime.matches("\\d+")){
      val start = loginTime.toInt
      val end = logoutTime.toInt
      if (logoutTime != 0) {
        totalOnline = end - start
      }
    }
    totalOnline
  }
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logRaw: DataFrame = null
    var logPath: Array[String] = null
    if (hourly == "") {
      val logPatternPath = Constants.GAMELOG_DIR + "/ttq/[yyyyMMdd]/*/*_login_info_*.csv"
      if (logDate.startsWith("2017-07-04")) {
        logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate, 1)
      } else {
        logPath = PathUtils.generateLogPathDaily(logPatternPath, logDate)
      }
      logRaw = getCsvWithHeaderLog(logPath, "\\t")
    }

    val sf = Constants.FIELD_NAME
    logRaw = logRaw.withColumnRenamed("Account", sf.ID)
      .withColumnRenamed("Role ID", sf.RID)
      .withColumnRenamed("Role name", sf.ROLE_NAME)
      .withColumn(sf.LOG_DATE, convertToDateTime(col("Login date")))
      .filter($"log_date".contains(logDate))
      .withColumn(sf.ONLINE_TIME, calcTotalOnline(col("Login date"), col("Logout date")))
      .withColumnRenamed("Zone ID/Channel ID/Server ID", sf.SID)
      .withColumnRenamed("OSPlatformID", sf.OS).drop("Login date", "Logout date")
      .withColumn(sf.GAME_CODE,lit("hk"))
    logRaw
  }


  private val getSid = udf { (fileName: String) => {
    val searchStr = fileName.split("/")(7)
    val pattern = new Regex("(\\d+)")
    val result = (pattern findAllIn searchStr).mkString(",")
    result
  }
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var ccuRaw: DataFrame = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/ttq/[yyyyMMdd]/*/actor_online_log_*.csv"
      val CcuPath = PathUtils.generateLogPathDaily(patternPath, logDate, 1)
      ccuRaw = getCsvWithHeaderLog(CcuPath, "\\t")

    }
    var ccu = ccuRaw.select(input_file_name() as 'filename, ccuRaw("online_total"), ccuRaw("Log date"))
    val sf = Constants.FIELD_NAME
    ccu.withColumn(sf.LOG_DATE, convertToDateTime(col("Log date")))
      .withColumnRenamed("online_total", sf.CCU)
      .withColumn(sf.SID, getSid(col("filename")))
      .withColumn(sf.GAME_CODE,lit("hk"))
      .drop("filename", "Log date")

  }
}
