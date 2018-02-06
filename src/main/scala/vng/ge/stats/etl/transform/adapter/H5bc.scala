package vng.ge.stats.etl.transform.adapter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.{Common, PathUtils}

import scala.util.matching.Regex


/**
  * Created by quangctn on 08/02/2017.
  * Contact Point: hieumt
  * mrtg code:
  * Release date:
  * + tentative : midle january
  * + real:
  * logType
  * + Active : login
  * + Payment : char */
class H5bc extends FairyFormatter("h5bc") {

  import sparkSession.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run() -> close()
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/h5bc/[yyyyMMdd]/logs/local/*"
      val path = PathUtils.generateLogPathDaily(pattern, logDate, 1)
      raw = getRawLog(path)

    }
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length >= 15) {
        if (line(2).equalsIgnoreCase("login")) {

          rs = true
        }
      }
      rs
    }
    val field = Constants.FIELD_NAME
    val df = raw.map(line => line.split("\\|")).filter(line => filterLog(line)).map { r =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val obj = mapper.readValue[Map[String, Object]](r(14))
      val sid = r(1)
      val dateTime = r(0)
      val id = r(4)
      val rid = r(6)
      val ip = obj("ip").toString
      val action = "login"
      ("h5bc", id, sid, dateTime, rid, ip, action)
    }.toDF(field.GAME_CODE, field.ID, field.SID, field.LOG_DATE, field.RID, field.IP, field.ACTION)
    df/*.where(s"log_date like '%$logDate%'")*/


  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/h5bc/[yyyyMMdd]/logs/local/*"
      val path = PathUtils.generateLogPathDaily(pattern, logDate, 1)
      raw = getRawLog(path)

    }
    val filterLog = (line: Array[String]) => {
      var rs = false
      if (line.length >= 15) {
        if (line(2).equalsIgnoreCase("charge")) {

          rs = true
        }
      }
      rs
    }
    var convertChargeId = scala.collection.mutable.Map[Int, Int]()
    convertChargeId += (201 -> 20000, 201 -> 20000,
      202 -> 50000,
      203 -> 100000,
      204 -> 200000,
      205 -> 500000,
      206 -> 1000000,
      207 -> 2000000)

    val field = Constants.FIELD_NAME
    val df = raw.map(line => line.split("\\|")).filter(line => filterLog(line)).map { r =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      val obj = mapper.readValue[Map[String, Object]](r(14))
      val sid = r(1)
      val dateTime = r(0)
      val id = r(4)
      val rid = r(6)
      val chargeId = obj("chargeId").toString
      val money = convertChargeId.apply(chargeId.toInt)
      ("h5bc", id, sid, dateTime, rid, money, money)
    }.toDF(field.GAME_CODE, field.ID, field.SID, field.LOG_DATE, field.RID, field.GROSS_AMT, field.NET_AMT)
    df/*.where(s"log_date like '%$logDate%'")*/
  }


}

