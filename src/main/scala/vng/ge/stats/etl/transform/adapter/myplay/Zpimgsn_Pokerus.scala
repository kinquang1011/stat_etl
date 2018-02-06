package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 13/02/2017.
  */
class Zpimgsn_Pokerus extends MyplayFormatter ("zpimgsn_pokerus") {
  import sqlContext.implicits._
  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  def otherFunction(): Unit = {
    //countryMapping
    //mone
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {

    var paymentRaw: RDD[String] = null
    if (hourly == ""){
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_pokerSea_id*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath,logDate)
      paymentRaw = getRawLog(paymentPath)
    }
    val filterLog = (line: String) =>{
      var rs = false
      if (line.startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
      val dateTime = line(6)
      val money = line(4)
      val id = line(2)
      ("zpimgsn_pokerus", dateTime,id,money,money)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
    paymentDs

  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var logUserFromHive: DataFrame = null
    if(hourly == ""){
      val logQuery = s"select * from zpimgsn_pokerus.user where ds = '$logDate'"
      logUserFromHive = getHiveLog(logQuery)
    }
    var logDs = logUserFromHive.selectExpr("log_date as log_date","user_id as id")
    logDs = logDs.withColumn("game_code",lit("zpimgsn_pokerus"))
    logDs
  }

}
