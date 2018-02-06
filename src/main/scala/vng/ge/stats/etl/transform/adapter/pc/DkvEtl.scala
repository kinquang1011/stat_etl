package vng.ge.stats.etl.transform.adapter.pc

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, countDistinct, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 21/04/2017
  */
class DkvEtl extends DbgGameFormatter ("dkv") {
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }
//  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
//
//    var raw: RDD[String] = null
//    if (hourly == "") {
//      val ccuPattern = Constants.GAMELOG_DIR + "/tn/[yyyyMMdd]/*/ccu.csv"
//      val ccuPath = PathUtils.generateLogPathDaily(ccuPattern, logDate, numberOfDay = 1)
//      raw = getRawLog(ccuPath)
//    }else{
//
//    }
//
//    val getDateTime = (timestamp:String) => {
//      var rs = timestamp
//      if(timestamp!=null){
//        rs = DateTimeUtils.getDate(timestamp.toLong*1000)
//      }
//      rs
//    }
//
//    val gameFilter = (line: Array[String]) => {
//      var rs = true
//      if (getDateTime(line(1)).contains(logDate)) {
//        rs = true;
//      }
//      rs
//    }
//    val sf = Constants.FIELD_NAME
//    val ccuDs = raw.map(line => line.split("\\t")).filter(line => gameFilter(line)).map { line =>
//      val ccu = line(0)
//      val dateTime = getDateTime(line(1));
//      ("dkv", ccu, dateTime)
//    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
//    ccuDs
//  }


  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/tn/[yyyyMMdd]/*/login_logout.csv"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
      loginRaw = getRawLog(loginPath)
    }else {
      //      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      //      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      //      loginRaw = getRawLog(loginPath)
    }
    val getDateTime = (timestamp:String) => {
      var rs = timestamp
      if(timestamp!=null){
        rs = DateTimeUtils.getDate(timestamp.toLong*1000)
      }
      rs
    }

    val filterlog = (line:Array[String]) => {
      var rs = false
      if (getDateTime(line(3)).startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = getDateTime(line(3))
      val id = line(0)
      val serverId = line(5)
      val action = "login"
      ("dkv", dateTime,id,serverId,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.ACTION)


    loginDs
  }

  def testPuPayment(dateFrom:String,dateto:String): DataFrame ={
    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(dateFrom,dateto).toInt,dateto)
    var df: DataFrame = null
    listDate.foreach(date =>
      if(df!=null && df!=emptyDataFrame){
        df= df.union(getPaymentDs(date,""))
      }else{
        df = getPaymentDs(date,"")
      }
    )
    df = df.agg(countDistinct("id").as("PU1"))
    df
  }

  def testActivity(dateFrom:String,dateto:String): DataFrame ={
    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(dateFrom,dateto).toInt,dateto)
    var df: DataFrame = null
    listDate.foreach(date =>
      if(df!=null && df!=emptyDataFrame){
        df= df.union(getActivityDs(date,""))
      }else{
        df = getActivityDs(date,"")
      }
    )
    df = df.select("id","log_date")
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))

    df = df.groupBy("log_date").agg(countDistinct("id").as("a1")).orderBy(asc("log_date"))
    df

    //df.orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")

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
