package vng.ge.stats.etl.transform.adapter.pc

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{DbgGameFormatter}
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils


/**
  * Created by Tanaye on 5/8/17.
  */

class Jxw extends DbgGameFormatter("jxw") {

  import sqlContext.implicits._

  val sf = Constants.FIELD_NAME
  val gamecode = "jxw"
  val rate = 100


  def start(args: Array[String]): Unit = {
    initParameters(args)
    setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)
    this -> run -> close
  }

  def filter(row: Row, logdate: String): Boolean = {
    row.getAs[String]("logdate").substring(0, 10).equals(logdate)
  }


  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var ccuDf: DataFrame = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/jxw/[yyyy_MM_dd]/vltkw_*/ccu.txt.gz"
      val CcuPath = PathUtils.generateLogPathDailyWithFormat(patternPath, logDate, "yyyy_MM_dd")
      ccuDf = getCsvWithHeaderLog(CcuPath, "\t")
    }

    ccuDf = ccuDf.withColumn("gamecode", lit(gamecode))
    ccuDf = ccuDf.select("gamecode", "logdate", "count", "serverid")
    ccuDf = ccuDf.withColumn("logdate", MyUdf.ccuTimeDropSeconds(col("logdate")))
    val ccuDs = ccuDf
      .filter($"logdate".startsWith(logDate))
      //      .toDF(sf.GAME_CODE, "date", sf.CCU, sf.SID)
      //      .withColumn(sf.LOG_DATE, MyUdf.ccuTimeDropSeconds(col("date")))
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.CCU, sf.SID)
    ccuDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var activityDf: DataFrame = null
    var characterInfoDf: DataFrame = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/jxw/[yyyy_MM_dd]/vltkw_*/roleLoginLogout.txt.gz"
      val activityPath = PathUtils.generateLogPathDailyWithFormat(patternPath, logDate, "yyyy_MM_dd")

      val patternCharacterInfoPath = Constants.GAMELOG_DIR + "/jxw/[yyyy_MM_dd]/vltkw_*/characterInfo.txt.gz"
      val characterInfoPath = PathUtils.generateLogPathDailyWithFormat(patternCharacterInfoPath, logDate, "yyyy_MM_dd")

      characterInfoDf = getCsvWithHeaderLog(characterInfoPath, "\t")
      activityDf = getCsvWithHeaderLog(activityPath, "\t")
    }

    activityDf = activityDf.withColumn("gamecode", lit(gamecode))
    activityDf = activityDf.select("gamecode", "logdate", "actorid", "serverid", "counter")
    activityDf = activityDf.as('a).join(characterInfoDf.as('b), activityDf("actorid") === characterInfoDf("actorid"), "left_outer")
    activityDf = activityDf.select("a.gamecode", "a.logdate", "a.actorid", "a.serverid", "a.counter", "b.accountid")

    val activityDs = activityDf
      .filter($"logdate".startsWith(logDate))
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.RID, sf.SID, sf.ACTION, sf.ID)
    activityDs

  }
}