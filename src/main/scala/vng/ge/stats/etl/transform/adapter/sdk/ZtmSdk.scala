package vng.ge.stats.etl.transform.adapter.sdk

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.SdkFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 06/02/2017.
  */
class ZtmSdk extends SdkFormatter("ztm") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    if (mapParameters.contains("sdkGameCode")) {
      sdkGameCode = mapParameters("sdkGameCode")
      sdkGameCode = sdkGameCode.toUpperCase
    }
    if (mapParameters.contains("gameCode")) {
      gameCode = mapParameters("gameCode").toLowerCase
    }
    if (mapParameters.contains("timezone")) {
      timezone = mapParameters("timezone")
    }
    if (mapParameters.contains("sdkSource")) {
      sdkSource = mapParameters("sdkSource")
    }
    if (mapParameters.contains("changeRate")) {
      changeRate = mapParameters("changeRate").toDouble
    }
    this -> run -> close
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var CcuRaw: RDD[String] = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/ztm/[yyyyMMdd]/ccu/ccu_[yyyyMMdd].csv"
      val CcuPath = PathUtils.generateLogPathDaily(patternPath, logDate)
      CcuRaw = getRawLog(CcuPath)
    }
    val filterPayTime = (line: Array[String]) => {
      var rs = false
      if (line.length >= 4) {
        if (MyUdf.timestampToDate((line(4).toLong) * 1000).startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    var CcuDs = CcuRaw.map(line => line.split("\\t")).filter(line => filterPayTime(line)).map { r =>
      val timeStamps = r(4).toLong
      val ccu = r(3)
      val sid = r(0)
      val datetime = MyUdf.timestampToDate(timeStamps * 1000)
      ("ztm", datetime, ccu, sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.CCU, sf.SID)
    CcuDs
  }
}
