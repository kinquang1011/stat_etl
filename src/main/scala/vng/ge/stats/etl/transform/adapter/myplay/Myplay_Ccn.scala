package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 09/02/2017.
  */
class Myplay_Ccn extends MyplayFormatter ("myplay_ccn"){
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var logRaw : RDD[String] = null
    if(hourly == ""){
      val logPatternPath = Constants.WAREHOUSE_DIR +"/ccn/unknown_log/[yyyy-MM-dd]/*"
      val logPath = PathUtils.generateLogPathDaily(logPatternPath,logDate)
      logRaw = getRawLog(logPath)
    }
    val filterLog = (line: String) => {
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val logDs = logRaw.map(line => line.split("\\t")).filter(line => filterLog(line(0))).map{ line =>
      val datetime = line(0)
      val id = line(1)
      ("myplay_ccn", datetime, id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    logDs
  }
}
