package vng.ge.stats.etl.transform.adapter.pc

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 05/05/2017.
  */
class Jx1 extends DbgGameFormatter("jx1"){
  //must set AppId array
  import sqlContext.implicits._


  def start(args: Array[String]): Unit = {
    initParameters(args)
    if("buildTotalData".equalsIgnoreCase(_logType)){
      this->createTotalData(_logDate)
    }else{
      this -> run -> close
    }
  }
//  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
//    import sparkSession.implicits._
//    val patternNewReg = Constants.GAMELOG_DIR + "/gslog/jx1/[yyyy-MM-dd]/new_register.*.csv"
//    val pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate, 1)
//    val rawNewReg = getRawLog(pathNewReg)
//    val newRegFilter = (line:Array[String]) => {
//      var rs = false
//      val loginDate = line(1)
//      if (loginDate.startsWith(logDate)) {
//        rs = true
//      }
//      rs
//    }
//    val sf = Constants.FIELD_NAME
//
//    val newRegDf = rawNewReg.map(line => line.split("\\t")).filter(line => newRegFilter(line)).map { line =>
//      val date = line(1)
//      val accountName = line(0)
//      ("jx1",date,accountName)
//    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
//
//
//    newRegDf
//  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val patternPathLogin = Constants.GAMELOG_DIR + "/gslog/jx1/[yyyy-MM-dd]/*/login_logout.*.csv"
      val loginPath = PathUtils.generateLogPathDaily(patternPathLogin, logDate)
      loginRaw = getRawLog(loginPath)

    }else {
      //      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      //      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      //      loginRaw = getRawLog(loginPath)
    }

    val filterlog = (line:Array[String]) => {
      var rs = false
      if (line(0).startsWith(logDate)){
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(0)
      val id = line(1)
      val rolename= line(2)
      val level = line(4)
      val action = line(5)
      val serverId = line(10)
      ("jx1", dateTime,id,serverId,action,rolename,level)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.ACTION,sf.RID,sf.LEVEL)

    loginDs
  }

  //  jx.testActivity("2017-01-01","2017-01-02").orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")

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

  def testPuPayment(dateFrom:String,dateto:String): Unit ={
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
        df = getActivityDs(date,"")
      }
    )
    df = df.select("id","log_date")
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    df.groupBy("log_date").agg(countDistinct("id")).orderBy(asc("log_date")).show(60);
  }

  def testRevPayment(dateFrom:String,dateto:String): Unit ={
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
        df = getActivityDs(date,"")
      }
    )
    df = df.select("net_amt","log_date")
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    df.groupBy("log_date").agg(sum("net_amt").cast("long")).orderBy(asc("log_date")).show(60);
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
