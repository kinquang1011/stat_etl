package vng.ge.stats.etl.transform.adapter.pc

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}
import org.apache.spark.sql.functions._

/**
  * Created by canhtq on 05/05/2017.
  */
class Nkvn extends DbgGameFormatter("nkvn"){

  def start(args: Array[String]): Unit = {
    initParameters(args)
    if("buildTotalData".equalsIgnoreCase(_logType)){
      this->createTotalData(_logDate)
    }else{
      this -> run -> close
    }
  }


  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    import sparkSession.implicits._
    val patternNewCharPath = Constants.GAMELOG_DIR + "/gslog/vlcm/[yyyy-MM-dd]/character_info/*"
    val pathNewChar = PathUtils.generateLogPathDaily(patternNewCharPath, logDate, 1)
    val rawChar = getRawLog(pathNewChar)
    val charsFilter = (line:Array[String]) => {
      var rs = false
      if (line.length > 12) {
        val loginDate = line(12)
        if (loginDate.startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }
    val accountFilter = (line:Array[String]) => {
      var rs = false
      if (line.length > 2) {
        val loginDate = line(0)
        if (loginDate.startsWith(logDate)) {
          rs = true
        }
      }
      rs
    }
    val charDs = rawChar.map(line => line.split("\\t")).filter(line => charsFilter(line)).map { line =>
      val date = line(0)
      val sid = line(15)
      val rid = line(1)
      val accountId = line(3)
      val roleName = line(4)
      val level = line(6)
      (sid,rid,accountId,roleName,level,date)
    }.toDF("sid","rid","account_id","role_name","level","log_date")

    val patternAcountPath = Constants.GAMELOG_DIR + "/gslog/vlcm/[yyyy-MM-dd]/account/*"
    val pathAccount = PathUtils.generateLogPathDaily(patternAcountPath, logDate,1)
    val rawAccount = getRawLog(pathAccount)
    val accountDs = rawAccount.map(line => line.split("\\t")).filter(line => accountFilter(line)).map { line =>
      val account_id = line(1)
      val account_name = line(2)
      (account_id,account_name)
    }.toDF("a_id","account_name")

    val minDs = accountDs.sort("a_id").dropDuplicates("a_id")
    val sf = Constants.FIELD_NAME
    val newDs = charDs.as('char).join(minDs.as('account),charDs("account_id")===minDs("a_id")).withColumn("game_code",lit(gameCode))
      .selectExpr("game_code","log_date","account_name as id","sid","rid","level")
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID,  sf.RID, sf.LEVEL)
    newDs
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    import sparkSession.implicits._
    val patternPath = Constants.GAMELOG_DIR + "/gslog/vlcm/[yyyy-MM-dd]/login/*"
    val path = PathUtils.generateLogPathDaily(patternPath, logDate)
    val raw = getRawLog(path)
    val filterLoginLogout = (line:Array[String]) => {
      var rs = false
      val loginDate = line(4)
      if (loginDate.startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val rsDs = raw.map(line => line.split("\\t")).filter(line => filterLoginLogout(line)).map { line =>
      val acn = line(1)
      val rid = line(2)
      val level = line(3).toInt
      val loginDate = line(4)
      val ip = line(5)
      val sid = line(7)
      ("nkvn",loginDate, acn, rid, level, sid,ip)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.LEVEL, sf.SID, sf.IP)
    rsDs

  }


  def test(): Unit ={
    val spark = sparkSession

  }

  def testCcuDs(dateFrom:String,dateto:String): DataFrame ={
    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(dateFrom,dateto).toInt,dateto)
    var df: DataFrame = null
    listDate.foreach(date =>
      if(df!=null && df!=emptyDataFrame){
        df= df.union(getCcuDs(date,""))
      }else{
        df = getCcuDs(date,"")
      }
    )
    df = df.withColumn("log_date_cal",ccuTimeDropHour(col("log_date")))
    df = df.select("ccu","log_date","log_date_cal")
    df = df.groupBy("log_date","log_date_cal").agg(sum("ccu").as("ccu"))
    val dfMax = df.groupBy("log_date_cal").agg(max("ccu").as("pcu")).orderBy(asc("log_date_cal"))
    val dfAvg = df.groupBy("log_date_cal").agg(avg("ccu").as("acu")).orderBy(asc("log_date_cal"))

    var joinDF = dfAvg.as("a").join(dfMax.as("b"), dfAvg("log_date_cal") === dfMax("log_date_cal"), "inner")
    joinDF = joinDF.select("a.log_date_cal", "a.acu","b.pcu").orderBy(asc("log_date_cal"))
    joinDF = joinDF.toDF("log_date","acu","pcu")

    joinDF
  }

  def getListDateBefore(distance: Integer, logDate: String): List[String] = {

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(logDate)
    val calendar = Calendar.getInstance(Locale.UK);
    calendar.setTime(date);

    var listDate = List(format.format(calendar.getTime))
    for (i <- 1 to distance - 1) {

      calendar.add(Calendar.DATE, -1)
      listDate = listDate ::: List(format.format(calendar.getTime))
    }

    return listDate
  }
  def getDateDiff(fromDate: String, toDate: String): Long = {

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val fromdatef = format.parse(fromDate)
    val todatef = format.parse(toDate)


    val calfrom = Calendar.getInstance(Locale.UK);
    val calto = Calendar.getInstance(Locale.UK);
    calfrom.setTime(fromdatef)
    calto.setTime(todatef)

    val diff = calto.getTimeInMillis-calfrom.getTimeInMillis  ;
    val duration = diff / 1000 / 60 / 60 / 24

    return duration +1
  }

}
