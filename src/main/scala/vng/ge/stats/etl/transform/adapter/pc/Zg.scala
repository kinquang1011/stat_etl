package vng.ge.stats.etl.transform.adapter.pc


import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 05/05/2017.
  */
class Zg extends DbgGameFormatter("zg"){
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
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    import sparkSession.implicits._
    val patternNewReg = Constants.GAMELOG_DIR + "/gslog/gn/[yyyy-MM-dd]/*/user_detail.*.csv"
    val pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate)
    val rawNewReg = getRawLog(pathNewReg)
    val newRegFilter = (line:Array[String]) => {
      var rs = false
      if (line.length>=23 && line(3).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME

    val newRegDf = rawNewReg.map(line => line.split("\\t")).filter(line => newRegFilter(line)).map { line =>
      val dateTime = line(3)
      val id = line(1)
      ("zg",dateTime,id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    newRegDf
  }


  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val patternPathLogin = Constants.GAMELOG_DIR + "/gslog/gn/[yyyy-MM-dd]/*/user_detail.*.csv"
      val loginPath = PathUtils.generateLogPathDaily(patternPathLogin, logDate)
      loginRaw = getRawLog(loginPath)
    }else {
      //      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      //      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      //      loginRaw = getRawLog(loginPath)
    }

    val filterlog = (line:Array[String]) => {
      var rs = false
      if(line.length>=23 && line(14).startsWith(logDate)){
        rs = true

      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(14)
      val id = line(1)
      val serverId = line(21)
      val action = "login"
      ("zg", dateTime,id,id,serverId,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.ACTION)

    loginDs
  }

  def testNewRegister(dateFrom:String,dateto:String): DataFrame ={
    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(dateFrom,dateto).toInt,dateto)
    var df: DataFrame = null
    listDate.foreach(date =>
      if(df!=null && df!=emptyDataFrame){
        df= df.union(getIdRegisterDs(date))
      }else{
        df = getIdRegisterDs(date)
      }
    )
    df = df.select("id","log_date")
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    df = df.groupBy("log_date").agg(countDistinct("id").as("n1")).orderBy(asc("log_date"))
    df

    //df.orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")

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

  def getAllTest(dateFrom:String,dateto:String): DataFrame ={
    val dfCcu = testCcuDs(dateFrom,dateto)
    val dfN1 = testNewRegister(dateFrom,dateto)
    val dfA1 = testActivity(dateFrom,dateto)
    val dfPU1 = testPuPayment(dateFrom,dateto)
    val dfRev1 = testRevPayment(dateFrom,dateto)


    val joinDf = dfCcu.as("ccu").join(dfN1.as("n"), "log_date").join(dfA1.as("a"), "log_date").join(dfPU1.as("p"), "log_date").join(dfRev1.as("r"), "log_date").select(
      "log_date","a.a1","p.PU1","r.rev", "ccu.pcu","ccu.acu","n.n1"
    )

    joinDf
    //joinDf.orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")

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
    df = df.select("id","log_date")
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    df = df.groupBy("log_date").agg(countDistinct("id").as("PU1")).orderBy(asc("log_date"))
    df
  }

  def testRevPayment(dateFrom:String,dateto:String): DataFrame ={
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
    df = df.select("net_amt","log_date")
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    df = df.groupBy("log_date").agg(sum("net_amt").cast("long").as("rev")).orderBy(asc("log_date"))
    df
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
