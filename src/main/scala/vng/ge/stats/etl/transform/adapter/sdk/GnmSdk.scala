package vng.ge.stats.etl.transform.adapter.sdk

import vng.ge.stats.etl.transform.adapter.base.SdkFormatter

/**
  * Created by quangctn on 06/02/2017.
  */
class GnmSdk extends SdkFormatter("gnm") {

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
