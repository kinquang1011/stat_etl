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
class Omg2Sdk extends SdkFormatter("omg2") {

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
}
