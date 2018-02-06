package vng.ge.stats.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairySdkFormatter
import vng.ge.stats.etl.utils.{Common, DateTimeUtils}

import scala.util.Try
/**
  * Created by lamnt6 on 08/06/2017.
  */
/**
  * cal.getPartquetlog("2017-06-07",7,"cack").groupBy("channel","os").agg(countDistinct("id")).sort("os").show
cal.getSdklog("2017-06-07",7,"GNM").groupBy("channel","os").agg(countDistinct("id")).sort("os").show

  */
class CalculateData  extends FairySdkFormatter("sdk") {
  import sqlContext.implicits._


//  var joinDF = d5.as("a").join(d.as("b"), d5("id") === d("id"), "inner")


  // storeDS.coalesce(1).write.mode("overwrite").format("parquet").save(s"tmp/projectC_2017_06_29")
  //joinDf.orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")
  //var joinDF = activity.as("a").join(totaldata.as("b"), activity("id") === totaldata("id"), "left_outer").where("b.id is null")

  def checkDbgGame(): Unit = {
    /**
      *val df = sparkSession.read.option("delimiter","\t").csv("/ge/gamelogs/dbg/2017-07-{25,26}").filter("_c1 = '10313'").agg(sum("_c33"))
      *val dv = df.filter("_c26 = 'SUCCESSFUL'").filter($"_c0".contains("2017-07-26"))
      */


  }

  //split string
  val parseId = udf { (s: String) => {
    var r = s
      if(s.contains("@vng.win.163.com")){
        r = s.substring(0,s.lastIndexOf("@"))
      }
    r
  }
  }

  //tlbbm converRate
  val converRate = udf { (s13: String,s10:String) => {
    var amt = s13.toLong * 200
    if(s13 == "3000" && s10 < "50"){
      amt = 500000L
    }
    amt
  }
  }


  val dateGmt7 = udf { (dateTime: String) => {
    val timeStamp = DateTimeUtils.getTimestamp(dateTime)+7*60*60*1000
    DateTimeUtils.getDate(timeStamp)
  }
  }


  val timeStampTodate = udf { (s: Long) => {
    DateTimeUtils.getDate(s)
  }
  }

  val datetoTimeStamp = udf { (dateTime: String) => {
    val timeStamp = DateTimeUtils.getTimestamp(dateTime)
    timeStamp
  }
  }

//  def getActiveUserIn(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
//    var dailyLogin: DataFrame = null
//
//    val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)
//    /ge/warehouse/jxm/PlayerLogin/2017-06-21/PlayerLogin-0%2C031.gz
//
//    val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
//    val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)
//
//    dailyLogin = getJsonLogF(logPath)
//    dailyLogin =dailyLogin.select("userID","type","updatetime");
//
//    var ds: DataFrame = dailyLogin
//
//
//    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
//    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))
//
//    var datefirst: DataFrame = null
//
//    datefirst = getJsonLogF(ff)
//
//    if(datefirst.count()>0){
//      datefirst=datefirst.filter($"updatetime".contains(toDate)).select("userID","type","updatetime");
//
//      ds = dailyLogin.union(datefirst);
//    }
//
//    if(filteroneday){
//      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
//      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
//      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))
//
//      var dailyLoginOneDay: DataFrame = null
//
//      dailyLoginOneDay = getJsonLog(ff)
//
//      dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore));
//
//      ds = dailyLogin.union(dailyLoginOneDay);
//    }
//    ds=ds.toDF("id","channel","log_date")
//    ds=ds.filter($"channel".contains("GU"))
//    ds = ds.dropDuplicates("id")
//    ds
//  }


  def getJsonLogF(pathList: Array[String]): DataFrame = {
    Common.logger("getJsonLog, path = " + pathList.mkString(","))
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.json(pathList: _*)
    }
    rs
  }

  def getJsonLogF(path: String): DataFrame = {
    Common.logger("getJsonLog, path = " + path)
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.json(path)
    }
    rs
  }

  private def generateLogPathDaily(pattern: String, from: String, numberOfDay: Int = 2): Array[String] = {
    var arr: Array[String] = Array()
    var f = pattern.replace("[yyyy-MM-dd]", from)
    f = f.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", from))
    arr = arr ++ Array(f)
    for (i <- 1 until numberOfDay ) {
      val date = DateTimeUtils.getDateDifferent(i * -1, from, Constants.TIMING, Constants.A1)
      var ff = pattern.replace("[yyyy-MM-dd]", date)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", date))
      arr = arr ++ Array(ff)
    }
    arr
  }
//  storeDS.coalesce(1).write.mode(writeMode).format("parquet").save(outputPath)


  def countUserbyZaloWithRangeDate(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
    var dailyLogin: DataFrame = null

    val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)

    val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
    val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)

    dailyLogin = getJsonLogF(logPath)
    dailyLogin =dailyLogin.select("userID","type","updatetime");

    var ds: DataFrame = dailyLogin


    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))

    var datefirst: DataFrame = null

    datefirst = getJsonLogF(ff)

    if(datefirst.count()>0){
      datefirst=datefirst.filter($"updatetime".contains(toDate)).select("userID","type","updatetime");

      ds = dailyLogin.union(datefirst);
    }

    if(filteroneday){
      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))

      var dailyLoginOneDay: DataFrame = null

      dailyLoginOneDay = getJsonLog(ff)

      dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore));

      ds = dailyLogin.union(dailyLoginOneDay);
    }
    ds=ds.toDF("id","channel","log_date")
    ds=ds.filter($"channel".contains("ZL"))
    ds = ds.dropDuplicates("id")
    ds
  }


  // lay so luong nguoi choi truoc ngay 30/6 va sau ngay 30/6 nguon ge/fairy/warehouse
  def countUserbyGuestWithRangeDate(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
    var dailyLogin: DataFrame = null

    val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)

    val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
    val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)

    dailyLogin = getJsonLogF(logPath)
    dailyLogin =dailyLogin.select("userID","type","updatetime");

    var ds: DataFrame = dailyLogin


    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))

    var datefirst: DataFrame = null

    datefirst = getJsonLogF(ff)

    if(datefirst.count()>0){
      datefirst=datefirst.filter($"updatetime".contains(toDate)).select("userID","type","updatetime");

      ds = dailyLogin.union(datefirst);
    }

    if(filteroneday){
      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))

      var dailyLoginOneDay: DataFrame = null

      dailyLoginOneDay = getJsonLog(ff)

      dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore));

      ds = dailyLogin.union(dailyLoginOneDay);
    }
    ds=ds.toDF("id","channel","log_date")
    ds=ds.filter($"channel".contains("GU"))
    ds = ds.dropDuplicates("id")
    ds
  }

  // lay danh sach tai khoan choi game trong bao nhiu ngay voi channel la GU
  def getSdkbyGuest(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
    var dailyLogin: DataFrame = null

    val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)

    val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
    val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)

    dailyLogin = getJsonLogF(logPath)
    dailyLogin =dailyLogin.select("userID","type","updatetime");

    var ds: DataFrame = dailyLogin


    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))

    var datefirst: DataFrame = null

    datefirst = getJsonLogF(ff)

    if(datefirst.count()>0){
      datefirst=datefirst.filter($"updatetime".contains(toDate)).select("userID","type","updatetime");

      ds = dailyLogin.union(datefirst);
    }

    if(filteroneday){
      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))

      var dailyLoginOneDay: DataFrame = null

      dailyLoginOneDay = getJsonLog(ff)

      dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore));

      ds = dailyLogin.union(dailyLoginOneDay);
    }
    ds=ds.toDF("id","channel","log_date")
    ds=ds.filter($"channel".contains("GU"))
    ds = ds.dropDuplicates("id")
    ds
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
  def getPaymentPartquetlogFairy(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }
    var paymentDf: DataFrame = null

    val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/*/*.parquet"
    val logPath = generateLogPathDaily(pattern, toDate, numberofday)


    var arr: Array[String] = Array()
    arr = Array(toDate)

    for (i <- 1 until numberofday ) {
      val date = DateTimeUtils.getDateDifferent(i * -1, toDate, Constants.TIMING, Constants.A1)
      arr = arr ++ Array(date)
    }


    paymentDf = getParquetLog(logPath)
    paymentDf = paymentDf.select("id","net_amt","log_date").where($"log_date".isin(arr))
    paymentDf=paymentDf.toDF("id","rev","log_date")
    paymentDf
//    paymentDf = paymentDf.withColumn("log_date",ccuTimeDropHour(col("log_date")))
//    paymentDf.groupBy("log_date").agg(sum("net_amt").cast("long")).orderBy(asc("log_date")).show(60);
  }

  def getAllParquetLogFileFairy(fromDate: String, toDate: String,gameCode:String): DataFrame ={
    val dfCcu = getCcuPartquetlogFairy(fromDate,toDate,gameCode)
    val dfN1 = getN1CountPartquetlogFairy(fromDate,toDate,gameCode)
    val dfA1 = getAcPartquetlogFairy(fromDate,toDate,gameCode)
    val dfPU1 = getPuPartquetlogFairy(fromDate,toDate,gameCode)
    val dfRev1 = getRevPartquetlogFairy(fromDate,toDate,gameCode)
    val dfNp1 = getNP1CountPartquetlogFairy(fromDate,toDate,gameCode)

    val joinDf = dfCcu.as("ccu").join(dfN1.as("n"), "log_date").join(dfA1.as("a"), "log_date").join(dfPU1.as("p"), "log_date").join(dfRev1.as("r"), "log_date").join(dfNp1.as("np"),"log_date").select(
      "log_date","a.a1","p.pu1","r.rev", "ccu.pcu","ccu.acu","n.n1","np.np1"
    )

    joinDf
    //joinDf.orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")

  }

  def getRevPartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }
    var paymentDf: DataFrame = null
    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    listDate.foreach(date =>
      if(paymentDf!=null && paymentDf!=emptyDataFrame){
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+date+"/*.parquet"
        paymentDf= paymentDf.union(getParquetLog(pattern))
      }else{
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+date+"/*.parquet"
        paymentDf = getParquetLog(pattern)
      }
    )
    paymentDf = paymentDf.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    paymentDf = paymentDf.select("net_amt","log_date")
    paymentDf=  paymentDf.groupBy("log_date").agg(sum("net_amt").cast("long").as("rev")).orderBy(asc("log_date"))
    paymentDf
  }

  def getPuPartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }
    var paymentDf: DataFrame = null
    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    listDate.foreach(date =>
      if(paymentDf!=null && paymentDf!=emptyDataFrame){
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+date+"/*.parquet"
        paymentDf= paymentDf.union(getParquetLog(pattern))
      }else{
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+date+"/*.parquet"
        paymentDf = getParquetLog(pattern)
      }
    )
    paymentDf = paymentDf.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    paymentDf = paymentDf.select("id","log_date")
    paymentDf = paymentDf.groupBy("log_date").agg(countDistinct("id").as("pu1")).orderBy(asc("log_date"))
    paymentDf
  }


  def getAcPartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    var df: DataFrame = null
    listDate.foreach(date =>
    if(df!=null && df!=emptyDataFrame){
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/"+date+"/*.parquet"
        df= df.union(getParquetLog(pattern))
      }else{
      val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/"+date+"/*.parquet"
      df = getParquetLog(pattern)
      }
    )
    df = df.withColumn("log_date",ccuTimeDropHour(col("log_date")))
    df = df.select("id","log_date")
    df= df.groupBy("log_date").agg(countDistinct("id").as("a1")).orderBy(asc("log_date"))
    df
  }

  def getNP1CountPartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    var df: DataFrame = null
    listDate.foreach { date =>
      var dfTotal: DataFrame = null
      var dfactive: DataFrame = null

      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, date, Constants.TIMING, Constants.A1)

      dfTotal
      val patternTotal = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/first_charge_2/" + date + "/*.parquet"
      dfTotal = getParquetLog(patternTotal)

      if(dfTotal!=emptyDataFrame && dfactive!=emptyDataFrame){

        dfTotal = dfTotal.withColumn("log_date", ccuTimeDropHour(col("log_date")))
        dfTotal = dfTotal.select("id", "log_date")
        dfTotal=dfTotal.groupBy("log_date").agg(countDistinct("id").as("np1"));

        if(df!=null ){
          df = df.union(dfTotal)
        }else{
          df = dfTotal
        }

      }
    }
    df =  df.orderBy(asc("log_date"))
    df

  }
//  jx.testActivity("2017-01-01","2017-01-02").orderBy("log_date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jx1-crosscheck.csv")
  def getN1CountPartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    var df: DataFrame = null
    listDate.foreach { date =>
      var dfTotal: DataFrame = null
      var dfactive: DataFrame = null

      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, date, Constants.TIMING, Constants.A1)

      dfTotal
      val patternTotal = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/accregister_2/" + date + "/*.parquet"
      dfTotal = getParquetLog(patternTotal)

      if(dfTotal!=emptyDataFrame && dfactive!=emptyDataFrame){

        dfTotal = dfTotal.withColumn("log_date", ccuTimeDropHour(col("log_date")))
        dfTotal = dfTotal.select("id", "log_date")
        dfTotal=dfTotal.groupBy("log_date").agg(countDistinct("id").as("n1"));

        if(df!=null ){
          df = df.union(dfTotal)
        }else{
          df = dfTotal
        }

      }
    }
    df =  df.orderBy(asc("log_date"))
    df
  }

  def getCcuPartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): DataFrame = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }
    var df: DataFrame = null
    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    listDate.foreach(date =>
      if(df!=null && df!=emptyDataFrame){
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/ccu_2/"+date+"/*.parquet"
        df= df.union(getParquetLog(pattern))
      }else{
        val pattern = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/ccu_2/"+date+"/*.parquet"
        df = getParquetLog(pattern)
      }
    )
    df = df.withColumn("log_date_cal",ccuTimeDropHour(col("log_date")))
    df = df.select("ccu","log_date","log_date_cal")
    df = df.groupBy("log_date","log_date_cal").agg(sum("ccu").as("ccu"))
    val dfAvg =df.groupBy("log_date_cal").agg(avg("ccu").as("acu")).orderBy(asc("log_date_cal"))
    val dfMax = df.groupBy("log_date_cal").agg(max("ccu").as("pcu")).orderBy(asc("log_date_cal"))

    var joinDF = dfAvg.as("a").join(dfMax.as("b"), dfAvg("log_date_cal") === dfMax("log_date_cal"), "inner")
    joinDF = joinDF.select("a.log_date_cal", "a.acu","b.pcu").orderBy(asc("log_date_cal"))
    joinDF = joinDF.toDF("log_date","acu","pcu")

    joinDF
  }

  def getNP1PartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): Unit = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }

    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    var df: DataFrame = null
    listDate.foreach { date =>
      var dfTotal: DataFrame = null
      var dfactive: DataFrame = null

      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, date, Constants.TIMING, Constants.A1)

      val patternTotal = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/total_paid_acc_2/" + oneDayAgo + "/*.parquet"
      dfTotal = getParquetLog(patternTotal)

      val patternActivity = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/" + date + "/*.parquet"
      dfactive = getParquetLog(patternActivity)

      if(dfTotal!=emptyDataFrame && dfactive!=emptyDataFrame){

        dfTotal = dfTotal.withColumn("log_date", ccuTimeDropHour(col("log_date")))
        dfTotal = dfTotal.select("id", "log_date")

        dfactive = dfactive.withColumn("log_date", ccuTimeDropHour(col("log_date")))
        dfactive = dfactive.select("id", "log_date")

        var joinDF = dfactive.as("a").join(dfTotal.as("b"), dfactive("id") === dfTotal("id"), "left_outer").where("b.id is null")
        joinDF = joinDF.select("a.id", "a.log_date").dropDuplicates("id")
        joinDF=joinDF.groupBy("log_date").agg(countDistinct("id").as("countId"));

        if(df!=null ){
          df = df.union(joinDF)
        }else{
          df = joinDF
        }

      }
    }

    df.orderBy(asc("log_date"))show(60);

  }

  def getN1PartquetlogFairy(fromDate: String, toDate: String,gameCode:String,filteroneday:Boolean=false): Unit = {

    val ccuTimeDropHour = udf { (datetime: String) => {
      DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", datetime)
    }
    }


    val listDate=  getListDateBefore(getDateDiff(fromDate,toDate).toInt,toDate)
    var df: DataFrame = null
    listDate.foreach { date =>
      var dfTotal: DataFrame = null
      var dfactive: DataFrame = null

      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, date, Constants.TIMING, Constants.A1)

      val patternTotal = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/total_login_acc_2/" + oneDayAgo + "/*.parquet"
      dfTotal = getParquetLog(patternTotal)

      val patternActivity = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/" + date + "/*.parquet"
      dfactive = getParquetLog(patternActivity)

      if(dfTotal!=emptyDataFrame && dfactive!=emptyDataFrame){

        dfTotal = dfTotal.withColumn("log_date", ccuTimeDropHour(col("log_date")))
        dfTotal = dfTotal.select("id", "log_date")


        dfactive = dfactive.withColumn("log_date", ccuTimeDropHour(col("log_date")))
        dfactive = dfactive.select("id", "log_date")

        var joinDF = dfactive.as("a").join(dfTotal.as("b"), dfactive("id") === dfTotal("id"), "left_outer").where("b.id is null")
        joinDF = joinDF.select("a.id", "a.log_date").dropDuplicates("id")
        joinDF=joinDF.groupBy("log_date").agg(countDistinct("id").as("countId"));

        if(df!=null ){
          df = df.union(joinDF)
        }else{
          df = joinDF
        }

      }
    }

    df.orderBy(asc("log_date"))show(60);

  }


  def getActivityPartquetlog(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
    var dailyLogin: DataFrame = null

    val logPattern = Constants.WAREHOUSE_DIR + "/"+gameCode + "/ub/sdk_data/activity_2/[yyyy-MM-dd]/*.parquet"
    val logPath = generateLogPathDaily(logPattern, toDate,numberofday)

    dailyLogin = getParquetLog(logPath)
    dailyLogin =dailyLogin.select("id","channel","os","log_date");

    dailyLogin
//    var ds: DataFrame = dailyLogin


//    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
//    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))
//
//    var datefirst: DataFrame = null
//
//    datefirst = getParquetLog(ff)
//
//    datefirst=datefirst.filter($"log_date".isNotNull && $"log_date".contains(toDate)).select("id","channel","os","log_date");
//
//    ds = dailyLogin.union(datefirst);
//
//    if(filteroneday){
//      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
//      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
//      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))
//
//      var dailyLoginOneDay: DataFrame = null
//
//      dailyLoginOneDay = getJsonLog(ff)
//
//      dailyLoginOneDay=dailyLoginOneDay.filter(!$"log_date".contains(dateOneMore)).select("id","channel","os","log_date");
//
//      ds = dailyLogin.union(dailyLoginOneDay);
//    }
//    dailyLogin =dailyLogin.select("id","channel","os","log_date");
//
//
//    ds;
  }

  def getAuToSDKlog(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): Unit = {
    var dailyLogin: DataFrame = null

    val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)

    val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
    val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)

    dailyLogin = getJsonLog(logPath)
    dailyLogin =dailyLogin.select("userID","type","device_os","updatetime");

    var ds: DataFrame = dailyLogin


    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))

    var datefirst: DataFrame = null

    datefirst = getJsonLog(ff)

    if(datefirst.count()>0){
      datefirst=datefirst.filter($"updatetime".contains(toDate)).select("userID","type","device_os","updatetime");

      ds = dailyLogin.union(datefirst);
    }

    if(filteroneday){
      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))

      var dailyLoginOneDay: DataFrame = null

      dailyLoginOneDay = getJsonLog(ff)

      dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore));

      ds = dailyLogin.union(dailyLoginOneDay);
    }
    ds=ds.toDF("id","channel","os","log_date")
    ds.groupBy("channel","os").agg(countDistinct("id")).sort("os").show(30);
  }


   def getSdklog(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
    var dailyLogin: DataFrame = null

     val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)

     val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
      val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)

     dailyLogin = getJsonLog(logPath)
     dailyLogin =dailyLogin.select("userID","type","device_os","updatetime");

     var ds: DataFrame = dailyLogin


     var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
     ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))

     var datefirst: DataFrame = null

     datefirst = getJsonLog(ff)

     if(datefirst.count()>0){
       datefirst=datefirst.filter($"updatetime".contains(toDate)).select("userID","type","device_os","updatetime");

       ds = dailyLogin.union(datefirst);
     }

     if(filteroneday){
         val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
         var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
         ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))

         var dailyLoginOneDay: DataFrame = null

         dailyLoginOneDay = getJsonLog(ff)

         dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore));

         ds = dailyLogin.union(dailyLoginOneDay);
     }
     ds=ds.toDF("id","channel","os","log_date")
     ds;
  }
//cal.getSdklog("2017-06-07",7,"CFMOBILE").groupBy("channel","os").agg(countDistinct("id")).sort("os").show
  //createOrReplaceTempView("df")
  // spark.sql("select count(DISTINCT(id)),os from df where channel like 'unk%' group by os order by (os)").show

  //  GU_ZL / GU : Chơi ngay
//
//   ZM : ZingID
//
//  ZL : Zalo
//
//   GG : Google+
//
//  FB : Facebook

  def getSDKsealog(toDate: String, numberofday:Int,gameCode:String,filteroneday:Boolean=false): DataFrame = {
    var dailyLogin: DataFrame = null

    val datenext = DateTimeUtils.getDateDifferent(-1, toDate, Constants.TIMING, Constants.A1)

    val logPattern = Constants.GAME_LOG_DIR + "/" + "sdk_sea" + "/[yyyy-MM-dd]/" + gameCode + "_Login_InfoLog/" + gameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
    val logPath = generateLogPathDaily(logPattern, datenext,numberofday-1)

    dailyLogin = getJsonLog(logPath)
    dailyLogin =dailyLogin.select("userID","type","device_os","updatetime");

    var ds: DataFrame = dailyLogin


    var ff = logPattern.replace("[yyyy-MM-dd]", toDate)
    ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", toDate))

    var datefirst: DataFrame = null

    datefirst = getJsonLog(ff)

    datefirst=datefirst.filter($"updatetime".isNotNull && $"updatetime".contains(toDate)).select("userID","type","device_os","updatetime");

    ds = dailyLogin.union(datefirst);

    if(filteroneday){
      val dateOneMore = DateTimeUtils.getDateDifferent((numberofday) * -1, toDate, Constants.TIMING, Constants.A1)
      var ff = logPattern.replace("[yyyy-MM-dd]", dateOneMore)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", dateOneMore))

      var dailyLoginOneDay: DataFrame = null

      dailyLoginOneDay = getJsonLog(ff)

      dailyLoginOneDay=dailyLoginOneDay.filter(!$"updatetime".contains(dateOneMore)).select("userID","type","updatetime","device_os");

      ds = dailyLogin.union(dailyLoginOneDay);
    }
    dailyLogin =dailyLogin.select("id","channel","os","log_date");

    ds=ds.toDF("id","channel","os","log_date")

    ds;

  }


}
