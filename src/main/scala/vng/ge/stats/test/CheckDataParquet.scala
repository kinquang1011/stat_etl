package vng.ge.stats.test


import org.apache.spark.sql.functions._


import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairySdkFormatter
import vng.ge.stats.etl.utils.DateTimeUtils
/**
  * Created by lamnt6 on 08/06/2017.
  */
/**
  * cal.getPartquetlog("2017-06-07",7,"cack").groupBy("channel","os").agg(countDistinct("id")).sort("os").show
cal.getSdklog("2017-06-07",7,"GNM").groupBy("channel","os").agg(countDistinct("id")).sort("os").show

  */
class CheckDataParquet  extends FairySdkFormatter("sdk") {


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

  private def generateLogPathDaily(pattern: String, from: String, numberOfDay: Int = 1): Array[String] = {
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
//  def checkPaymentWarehouse(gameCode:String, logDate:String):DataFrame ={
//
//    val paymentPath = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+logDate+"/*.parquet"
//
//    val paymentDf = getParquetLog(paymentPath)
//
//    val net_amt = paymentDf.agg(sum("net_amt").cast("Long")).first.getLong(0)
//    val count = paymentDf.count()
//
//    var checkDate = "Not Ok"
//    var checkTransaction = "Not Ok"
//
//    if(paymentDf.where("log_date not like '%"+logDate+"%'").count()==0){
//      checkDate="It's OK"
//    }
//
//    if(count==paymentDf.select("trans_id").dropDuplicates("trans_id").count()){
//      checkTransaction="It's OK"
//    }
//    println("Số net_amt: "+net_amt)
//    println("Số Record: "+count)
//    println("Check log_date: .."+checkDate)
//    println("Check trans_id: .."+checkTransaction)
//
//    println("*******************")
//    paymentDf
//  }
def checkPaymentWarehouseSdk(gameCode:String, logDate:String):DataFrame ={

  val paymentPath = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/sdk_data/payment_2/"+logDate+"/*.parquet"

  val paymentDf = getParquetLog(paymentPath)

  val net_amt = paymentDf.agg(sum("net_amt").cast("Long")).first.getLong(0)
  val count = paymentDf.count()

  var checkDate = "Not Ok"
  var checkTransaction = "Not Ok"


  if(paymentDf.where("log_date not like '%"+logDate+"%'").count()==0){
    checkDate="It's OK"
  }

  if(count==paymentDf.select("trans_id").dropDuplicates("trans_id").count()){
    checkTransaction="It's OK"
  }

  paymentDf.withColumn("log_date_cal",substring(col("log_date"),0,13)).select("log_date_cal").distinct.orderBy("log_date_cal").show(30)

  println("Số net_amt: "+net_amt)
  println("Số Record: "+count)
  println("Check log_date: .."+checkDate)
  println("Check trans_id: .."+checkTransaction)

  println("*******************")
  paymentDf
}

  def checkPaymentWarehouse(gameCode:String, logDate:String):DataFrame ={

    val paymentPath = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+logDate+"/*.parquet"

    val paymentDf = getParquetLog(paymentPath)

    val net_amt = paymentDf.agg(sum("net_amt").cast("Long")).first.getLong(0)
    val count = paymentDf.count()

    var checkDate = "Not Ok"
    var checkTransaction = "Not Ok"


    if(paymentDf.where("log_date not like '%"+logDate+"%'").count()==0){
      checkDate="It's OK"
    }

    if(count==paymentDf.select("trans_id").dropDuplicates("trans_id").count()){
      checkTransaction="It's OK"
    }

    paymentDf.withColumn("log_date_cal",substring(col("log_date"),0,13)).select("log_date_cal").distinct.orderBy("log_date_cal").show(30)

    println("Số net_amt: "+net_amt)
    println("Số Record: "+count)
    println("Check log_date: .."+checkDate)
    println("Check trans_id: .."+checkTransaction)

    println("*******************")
    paymentDf
  }

  def checkPayment(gameCode:String, logDate:String):DataFrame ={

    val paymentPath = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/payment_2/"+logDate+"/*.parquet"

    val paymentDf = getParquetLog(paymentPath)

    val net_amt = paymentDf.agg(sum("net_amt").cast("Long")).first.getLong(0)
    val count = paymentDf.count()

    var checkDate = "Not Ok"
    var checkTransaction = "Not Ok"


    if(paymentDf.where("log_date not like '%"+logDate+"%'").count()==0){
      checkDate="It's OK"
    }

    if(count==paymentDf.select("trans_id").dropDuplicates("trans_id").count()){
      checkTransaction="It's OK"
    }

    paymentDf.withColumn("log_date_cal",substring(col("log_date"),0,13)).select("log_date_cal").distinct.orderBy("log_date_cal").show(30)

    println("Số net_amt: "+net_amt)
    println("Số Record: "+count)
    println("Check log_date: .."+checkDate)
    println("Check trans_id: .."+checkTransaction)

    println("*******************")
    paymentDf
  }
  def printResult =(net_amt:Long,count:Long)=>{
    println("Số net_amt: "+net_amt)
    println("Số Record: "+count)

  }
  def checkRROs(gameCode:String, logDate:String, number:Integer):DataFrame ={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance(Locale.UK);
    val date = format.parse(logDate)
    calendar.setTime(date);

    calendar.add(Calendar.DATE, -1*number)
    val prevDate = format.format(calendar.getTime)

    val prevPath = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/accregister_2/"+prevDate+"/*.parquet"

    val activePath = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/"+logDate+"/*.parquet"


    val activeDf = getParquetLog(activePath).select("id","os").dropDuplicates("id")

    val prevDF = getParquetLog(prevPath).select("id","os").dropDuplicates("id")

    val joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    val retention = joinDF.distinct().count()

    val prevActive = prevDF.select("id").count()
    var rate = 0.0

    if(prevActive != 0) {
      rate = retention * 100.0 / prevActive
    }

    val retentionAndroid = joinDF.where("pa.os = 'android'").distinct().count()
    val prevAndroid = prevDF.where("os ='android'").count()
    var rateAndroid = 0.0

    if(prevAndroid != 0) {
      rateAndroid = retentionAndroid * 100.0 / prevAndroid
    }

    val retentionIos = joinDF.where("pa.os = 'ios'").distinct().count()
    val prevIos = prevDF.where("os ='ios'").count()
    var rateIos= 0.0


    if(prevIos != 0) {
      rateIos = retentionIos * 100.0 / prevIos
    }
    println("RR"+number+": "+rate+"%")

    println("Android RR"+number+": "+rateAndroid+"%")

    println("IOS RR"+number+": "+rateIos+"%")

    joinDF

  }


  def checkRR(gameCode:String, logDate:String, number:Integer):DataFrame ={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance(Locale.UK);
    val date = format.parse(logDate)
    calendar.setTime(date);

    calendar.add(Calendar.DATE, -1*number)
    val prevDate = format.format(calendar.getTime)

    val patternPrev = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/accregister_2/[yyyy-MM-dd]/*.parquet"

    val patternActive = Constants.FAIRY_WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/[yyyy-MM-dd]/*.parquet"

    val activePath = generateLogPathDaily(patternActive, logDate)

    val prevPath = generateLogPathDaily(patternPrev, prevDate)


    val activeDf = getParquetLog(activePath).select("id").distinct()

    val prevDF = getParquetLog(prevPath).select("id").distinct()

    val joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    val retention = joinDF.distinct().count()

    val prevActive = prevDF.count()
    var rate = 0.0

    if(prevActive != 0) {
      rate = retention * 100.0 / prevActive
    }
    println(prevDate)

    println("RR"+number+": "+rate+"%")

    joinDF
  }
  def checkRRWarehouse(gameCode:String, logDate:String, rr60:Integer=60, rr90:Integer=90, rr120:Integer=120, rr150:Integer=150, rr180:Integer=180):DataFrame ={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(logDate)




    val patternPrev = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/data/accregister_2/[yyyy-MM-dd]/*.parquet"

    val patternActive = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/[yyyy-MM-dd]/*.parquet"
    val activePath = generateLogPathDaily(patternActive, logDate)

    val activeDf = getParquetLog(activePath).select("id").distinct()

    val calendar60 = Calendar.getInstance(Locale.UK);
    calendar60.setTime(date);
    calendar60.add(Calendar.DATE, -1*rr60)

    var prevDate = format.format(calendar60.getTime)

    var prevPath = generateLogPathDaily(patternPrev, prevDate)

    var prevDF = getParquetLog(prevPath).select("id").distinct()

    var joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    var retention = joinDF.distinct().count()

    var prevActive = prevDF.count()
    var rate60 = 0.0

    if(prevActive != 0) {
      rate60 = retention * 100.0 / prevActive
    }


    val calendar90 = Calendar.getInstance(Locale.UK);
    calendar90.setTime(date);
    calendar90.add(Calendar.DATE, -1*rr90)

    prevDate = format.format(calendar90.getTime)

    prevPath = generateLogPathDaily(patternPrev, prevDate)

    prevDF = getParquetLog(prevPath).select("id").distinct()

    joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    retention = joinDF.distinct().count()

    prevActive = prevDF.count()
    var rate90 = 0.0

    if(prevActive != 0) {
      rate90 = retention * 100.0 / prevActive
    }

//
    val calendar120 = Calendar.getInstance(Locale.UK);
    calendar120.setTime(date);
    calendar120.add(Calendar.DATE, -1*rr120)
    prevDate = format.format(calendar120.getTime)

    prevPath = generateLogPathDaily(patternPrev, prevDate)

    prevDF = getParquetLog(prevPath).select("id").distinct()

    joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    retention = joinDF.distinct().count()

    prevActive = prevDF.count()
    var rate120 = 0.0

    if(prevActive != 0) {
      rate120 = retention * 100.0 / prevActive
    }
//
    val calendar150 = Calendar.getInstance(Locale.UK);
    calendar150.setTime(date);
    calendar150.add(Calendar.DATE, -1*rr150)
    prevDate = format.format(calendar150.getTime)

    prevPath = generateLogPathDaily(patternPrev, prevDate)

    prevDF = getParquetLog(prevPath).select("id").distinct()

    joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    retention = joinDF.distinct().count()

    prevActive = prevDF.count()
    var rate150 = 0.0

    if(prevActive != 0) {
      rate150 = retention * 100.0 / prevActive
    }
//
    val calendar180 = Calendar.getInstance(Locale.UK);
    calendar180.setTime(date);
    calendar180.add(Calendar.DATE, -1*rr180)
    prevDate = format.format(calendar180.getTime)

    prevPath = generateLogPathDaily(patternPrev, prevDate)

    prevDF = getParquetLog(prevPath).select("id").distinct()

    joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    retention = joinDF.distinct().count()

    prevActive = prevDF.count()

    println(prevDate);


    var rate180 = 0.0

    if(prevActive != 0) {
      rate180 = retention * 100.0 / prevActive
    }

    //    println(prevDate)
//
//    println("RR"+number+": "+rate+"%")


    val result = sqlContext.createDataFrame(Seq(
      (logDate, rate60,rate90,rate120,rate150,rate180)
    )).toDF("log_date","rr60","rr90","rr120","rr150","rr180")
    result
  }
  def checkRRInRange(dateFrom:String,dateto:String,gameCode:String): DataFrame ={


    val listDate=  getListDateBefore(getDateDiff(dateFrom,dateto).toInt,dateto)
    var df: DataFrame = null
    listDate.foreach(date =>
      if(df!=null && df!=emptyDataFrame){
        df= df.union(checkRRWarehouse(gameCode,date))
      }else{
        df = checkRRWarehouse(gameCode,date)
      }
    )
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


  def checkRRWarehouseRange(gameCode:String, logDate:String, number:Integer):DataFrame ={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance(Locale.UK);
    val date = format.parse(logDate)
    calendar.setTime(date);

    calendar.add(Calendar.DATE, -1*number)
    val prevDate = format.format(calendar.getTime)

    val patternPrev = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/data/accregister_2/[yyyy-MM-dd]/*.parquet"

    val patternActive = Constants.WAREHOUSE_DIR + "/" + gameCode + "/ub/data/activity_2/[yyyy-MM-dd]/*.parquet"

    val activePath = generateLogPathDaily(patternActive, logDate)

    val prevPath = generateLogPathDaily(patternPrev, prevDate)


    val activeDf = getParquetLog(activePath).select("id").distinct()

    val prevDF = getParquetLog(prevPath).select("id").distinct()

    val joinDF = prevDF.as('pa).join(activeDf.as('ca), prevDF("id") === activeDf("id"), "inner").select("pa.id")

    val retention = joinDF.distinct().count()

    val prevActive = prevDF.count()
    var rate = 0.0

    if(prevActive != 0) {
      rate = retention * 100.0 / prevActive
    }
    println(prevDate)

    println("RR"+number+": "+rate+"%")

    joinDF
  }

}
