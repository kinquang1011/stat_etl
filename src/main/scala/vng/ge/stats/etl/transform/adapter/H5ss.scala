package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.utils.{Common, PathUtils}

import scala.util.Try
import scala.util.matching.Regex

/**
  * Created by quangctn on 23/02/2017.
  * Release : 13/09
  * MRTG Code : 284
  */
//contact point : tanpnt
class H5ss extends FairyFormatter("h5ss") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  /*private val getSid = udf { (fileName: String) => {
    Common.logger("sid")
    val pattern = new Regex("(s\\d+)")
    val result = (pattern findAllIn fileName).mkString(",")
    result.substring(1)
  }
  }*/
  private val getDateTime = udf { (dateTime: String) => {
    val newDate = dateTime.substring(0, dateTime.length() - 5).replaceAll("T", " ")
    newDate
  }
  }
  private val getUTMSouce = udf { (row: String) => {

    var utmSource = ""
    Try {
      val pattern = new Regex("utm=(.+?)&|utm=[aA-z]+")
      utmSource = (pattern findAllIn row).mkString(",")
      if (!utmSource.equalsIgnoreCase("")) {
        utmSource = utmSource.replaceAll("utm=", "").replaceAll("&", "")
      }
    }
    utmSource
  }
  }
  /* val makeBlankIfNull = udf {(str: String) => {
     if (str == null ) "" else str
   }
   }*/

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var accJson: DataFrame = null
    var userJson: DataFrame = null
    var recordJson: DataFrame = null
    var resultDf: DataFrame = null
    val sf = Constants.FIELD_NAME
    val query1 = s"Select id,name from h5ss.account where ds like '%$logDate%' "
    accJson = getHiveLog(query1)
    /*Common.logger("count: "+accJson.count())*/
    val query2 = s"select  id, accid,serverid from h5ss.user where ds like '%$logDate%'"
    userJson = getHiveLog(query2)
    /*Common.logger("count: "+userJson.count())*/
    val query3 = s"select  user_id,log_date from h5ss.record where ds like '%$logDate%' and type =2"
    recordJson = getHiveLog(query3)
    val record_accid = recordJson.as('a).join(userJson.as('b),recordJson("user_id") === userJson("id"), "left_outer")
    .where("b.id is not null")
    var join = accJson.as('a).join(record_accid.as('b), accJson("id") === record_accid("accid"), "left_outer").where("accid is not null")
      .select("b.id", "accid", "serverid", "name","b.log_date")
    join = join.withColumnRenamed("accid", sf.RID)
      .withColumnRenamed("serverid", sf.SID)
      .withColumn(sf.GAME_CODE, lit("h5ss"))
    join = join.where(s"log_date like '%$logDate%'").withColumn("sourceId", regexp_replace(col("name"), "5Wan-178_", "")).drop("name")
    /*Common.logger("count: "+join.count())*/
    resultDf = join
    var sourceDf: DataFrame = null
    if (logDate.replace("-", "") >= "20171030") {
      val logAttactSourcePtn = Constants.GAMELOG_DIR + "/h5ss/[yyyyMMdd]/login_log/user_log.csv"
      val path = PathUtils.generateLogPathDaily(logAttactSourcePtn, logDate, 1)
      sourceDf = getCsvWithHeaderLog(path, ",").select("uid", "source").dropDuplicates("uid")
    } else if (logDate.replace("-", "") < "20171030") {
      val pathSource = "/ge/gamelogs/h5ss/user_log_full.csv"
      val path = PathUtils.generateLogPathDaily(pathSource, logDate, 1)
      sourceDf = getCsvWithHeaderLog(path, ",").select("uid", "source", "login_date")
        .where(s"login_date like '%$logDate%'").dropDuplicates("uid")
    }
    sourceDf = sourceDf.withColumn("newSource", getUTMSouce(col("source")))
    var joinSource = join.as('a).join(sourceDf.as('b), join("sourceId") === sourceDf("uid"), "left_outer").select("a.*", "b.newSource")
    joinSource = joinSource.withColumnRenamed("newSource", sf.CHANNEL).drop("newSource", "sourceId")
    resultDf = joinSource
    resultDf
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    val queryPym = s"select user_id, server_id, log_date, money, state,pay_id from h5ss.recharge where ds like '%$logDate%'  "
    var payment = getHiveLog(queryPym)
    payment = payment.withColumnRenamed("userid", sf.ID)
      .withColumnRenamed("serverid", sf.SID)
      .withColumn(sf.GROSS_AMT, col("money"))
      .withColumnRenamed("pay_id",sf.TRANS_ID)
      .withColumnRenamed("money", sf.NET_AMT)
      .withColumn(sf.GAME_CODE, lit("h5ss"))
      .withColumnRenamed("user_id", sf.ID)
    payment = payment.where(s"log_date like '%$logDate%' and state == 2 and trans_id!='自充'")
    payment
  }

  //TEST
  def getPaymentDs2(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var payment: DataFrame = null
    val payPattern = Constants.GAMELOG_DIR + "/h5ss/[yyyyMMdd]/[yyyyMMdd]/logs/real/account/recharge.json"
    val payPath = PathUtils.generateLogPathDaily(payPattern, logDate, 2)
    payment = getJsonLog(payPath).select("serverid", "userid", "time.$date", "money", "state")
    payment = payment.withColumnRenamed("serverid", sf.SID)
      .withColumnRenamed("userid", sf.ID)
      .withColumn(sf.GROSS_AMT, col("money"))
      .withColumnRenamed("money", sf.NET_AMT)
      .withColumn(sf.LOG_DATE, getDateTime(col("$date")))
      .drop("$date")
    payment = payment.where(s"log_date like '%$logDate%' and state == 2")
    payment.withColumn(sf.GAME_CODE, lit("h5ss"))
  }

  def getLoginLog(spark: SparkSession, logDate: String): DataFrame = {
    val logAttactSourcePtn = Constants.GAMELOG_DIR + "/h5ss/[yyyyMMdd]/login_log/user_log.csv"
    val path = PathUtils.generateLogPathDaily(logAttactSourcePtn, logDate, 1)
    var sourceDf = getCsvWithHeaderLog(path, ",").select("uid", "source")
    sourceDf = sourceDf.withColumn("newSource", getUTMSouce(col("source")))
    sourceDf
  }

  def testGetJoinInfo(payDs: DataFrame, actDs: DataFrame): DataFrame = {
    getJoinInfo(payDs, actDs)
  }
  def xxx(logDate: String, hourly: String): DataFrame = {

    val logAttactSourcePtn = Constants.GAMELOG_DIR + "/h5ss/[yyyyMMdd]/login_log/app_log.csv"
    val path = PathUtils.generateLogPathDaily(logAttactSourcePtn, logDate, 1)
    var sourceDf = getCsvWithHeaderLog(path, ",").select("uid", "source")
    sourceDf = sourceDf.withColumn("newSource", getUTMSouce(col("source")))
    Common.logger("xxx")
    sourceDf

  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var accJson: DataFrame = null
    var userJson: DataFrame = null
    var resultDf: DataFrame = null
    val sf = Constants.FIELD_NAME
    val query1 = s"Select id,name from h5ss.account where ds like '%$logDate%' "
    accJson = getHiveLog(query1)
    /*Common.logger("count: "+accJson.count())*/
    val query2 = s"select  id, accid, cretime,serverid from h5ss.user where ds like '%$logDate%'"
    userJson = getHiveLog(query2)
    /*Common.logger("count: "+userJson.count())*/
    var join = accJson.as('a).join(userJson.as('b), accJson("id") === userJson("accid"), "left_outer").where("accid is not null")
      .select("b.id", "accid", "cretime", "serverid", "name")
    join = join.withColumnRenamed("accid", sf.RID)
      .withColumnRenamed("serverid", sf.SID)
      .withColumnRenamed("cretime", sf.LOG_DATE)
      .withColumn(sf.GAME_CODE, lit("h5ss"))
    join = join.where(s"log_date like '%$logDate%'").withColumn("sourceId", regexp_replace(col("name"), "5Wan-178_", "")).drop("name")
    /*Common.logger("count: "+join.count())*/
    resultDf = join
    var sourceDf: DataFrame = null
    if (logDate.replace("-", "") >= "20171030") {
      val logAttactSourcePtn = Constants.GAMELOG_DIR + "/h5ss/[yyyyMMdd]/login_log/user_log.csv"
      val path = PathUtils.generateLogPathDaily(logAttactSourcePtn, logDate, 1)
      sourceDf = getCsvWithHeaderLog(path, ",").select("uid", "source").dropDuplicates("uid")
    } else if (logDate.replace("-", "") < "20171030") {
      val pathSource = "/ge/gamelogs/h5ss/user_log_full.csv"
      val path = PathUtils.generateLogPathDaily(pathSource, logDate, 1)
      sourceDf = getCsvWithHeaderLog(path, ",").select("uid", "source", "login_date")
        .where(s"login_date like '%$logDate%'").dropDuplicates("uid")
    }
    sourceDf = sourceDf.withColumn("newSource", getUTMSouce(col("source")))
    var joinSource = join.as('a).join(sourceDf.as('b), join("sourceId") === sourceDf("uid"), "left_outer").select("a.*", "b.newSource")
    joinSource = joinSource.withColumnRenamed("newSource", sf.CHANNEL).drop("newSource", "sourceId")
    resultDf = joinSource
    resultDf
  }
}


