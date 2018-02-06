package vng.ge.stats.etl.adhoc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, functions}
import org.apache.spark.sql.functions.{col, _}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.db.MysqlDB
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.utils.Common
import vng.ge.stats.report.util.IdConfig

import scala.collection.mutable
import scala.util.Try

/**
  * Created by canhtq on 30/03/2017.
  */
class AdhocQry {
  def mNewReg(): Unit = {
    val spark = SparkEnv.getSparkSession
    val mNRU = spark.read.parquet("/ge/warehouse/gnm/ub/data/accregister_2/2017-03-*")
    val mRev= spark.read.parquet("/ge/warehouse/gnm/ub/data/payment_2/2017-03-*")
    mNRU.createOrReplaceTempView("mNRU")
    mRev.createOrReplaceTempView("mRev")
    val mNRUpay = spark.sql("select mNRU.log_date as reg_date, mNRU.id, mRev.log_date as pay_date, mRev.trans_id,mRev.net_amt,mRev.sid from mNRU, mRev where mRev.id = mNRU.id")
    mNRUpay.createOrReplaceTempView("mNRUpay")
    spark.sql("select cast(sum(net_amt) as Long) from mNRUpay").show(false)
  }

  def test():Unit={
    import vng.ge.stats.etl.transform.adapter._
    val pl = new Phuclong()
    val logDate ="2017-03-29"
    pl.getActivityDs(logDate,"").show(false)
    pl.getPaymentDs(logDate,"").show(false)
    pl.getCcuDs(logDate,"").show(false)
  }

  def jxm(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession
    val month="2017-06"
    val totalMonth="2017-06-30"
    val allUserPath=s"/ge/warehouse/jxm/ub/data/total_login_acc_2/$totalMonth"
    val allDs= spark.read.parquet(allUserPath)
    val payPath=s"/ge/warehouse/jxm/ub/data/payment_2/$month-*"
    val payDs= spark.read.parquet(payPath)
    allDs.createOrReplaceTempView("allDs")
    payDs.createOrReplaceTempView("payDs")

    val mapDs = spark.sql("select payDs.*, allDs.log_date as reg_time from payDs LEFT OUTER JOIN allDs on payDs.id=allDs.id").withColumn("date",substring(col("log_date"),0,10)).withColumn("reg_date",substring(col("reg_time"),0,10))
    val rs = mapDs.where(s"date like '$month-%'").groupBy("id","reg_date","date").agg(sum("net_amt").as("rev"))
    rs.orderBy("date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jxm/$month-jxm-daily-rev.csv")

    val mtext="2017-06"
    val actPath=s"/ge/warehouse/jxm/ub/data/activity_2/$mtext-*"
    val actDs= spark.read.parquet(actPath).withColumn("date",substring(col("log_date"),0,10)).select("date","id","did","os")
    actDs.createOrReplaceTempView("actDs")
    val rrs = spark.sql("select date,os,count(distinct(id)) as active_user, count(distinct(did)) as active_device from actDs group by date, os")
    rrs.orderBy("date").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/jxm/$mtext-jxm-active.csv")

  }
  def sdkPay(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession
    val game="nikkisea"
    val pay = spark.read.parquet(s"/ge/warehouse/$game/ub/sdk_data/payment_2/*").withColumn("month",substring(col("log_date"),0,7))
    pay.createOrReplaceTempView("pay")
    val rs= spark.sql("select month,pay_channel, cast(sum(net_amt) as Long) as revenue from pay group by month,pay_channel")
    rs.show(false)
    rs.orderBy("pay_channel").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/$game/pay-channel.csv")
  }
  def cack(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession
    val game="cack"
    val nru = spark.read.parquet(s"/ge/warehouse/$game/ub/sdk_data/accregister_2/*").where("log_date >='2017-03-31'")
    val login = spark.read.json("/ge/gamelogs/sdk/2017-*/CACK_Login_InfoLog-2017-*.gz").where("updatetime>'2017-03-31'").select("userID","android_id","advertising_id","device_id","device_os").sort("userID").dropDuplicates("userID")
    nru.createOrReplaceTempView("nru")

    login.createOrReplaceTempView("login")
    val rs= spark.sql("select nru.id,login.* from nru, login where nru.id=login.userID")
    rs.createOrReplaceTempView("rs")
    val pay = spark.read.parquet(s"/ge/warehouse/$game/ub/sdk_data/payment_2/*").where("log_date >='2017-03-31'")
    pay.createOrReplaceTempView("pay")
    val fin= spark.sql("select rs.*, pay.net_amt from rs LEFT OUTER JOIN pay on rs.id=pay.id")
    fin.createOrReplaceTempView("fin")
    val csv= spark.sql("select id,android_id,device_id,advertising_id, cast(sum(net_amt) as Long) as revenue from fin group by id,android_id,device_id,advertising_id")
    csv.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/$game/data.csv")

    val payAll = spark.read.parquet(s"/ge/warehouse/$game/ub/sdk_data/payment_2/*")

    payAll.where("id='1008228974497179119'").show()
  }

  def checkNew(): Unit ={
    val spark = SparkEnv.getSparkSession
    val logDate="2017-05-05"
    val prevDate="2017-05-04"
    val gameCode="jxm"
    val active = spark.read.parquet(s"/ge/warehouse/$gameCode/ub/data/activity_2/$logDate").sort("id").dropDuplicates("id")
    val total = spark.read.parquet(s"/ge/warehouse/$gameCode/ub/data/total_login_acc_2/$prevDate")
    active.createOrReplaceTempView("active")
    total.createOrReplaceTempView("total")
    val nru= spark.sql("select total.id, active.id as nid from active left outer join total on total.id = active.id").where("id is null")
  }
  def checkSDK(): Unit ={
    val spark = SparkEnv.getSparkSession

    val login = spark.read.json("/ge/gamelogs/sdk/2017-05-28/CACK_Login_InfoLog-2017-05-28.gz")

  }
  def nikkithaiHourly(): Unit ={
    val spark = SparkEnv.getSparkSession
    import spark.implicits._
    import java.util.Calendar
    import java.text.SimpleDateFormat
    import java.util.Date
    import java.text.ParseException
    import java.util.Locale

    val cal=Calendar.getInstance
    val format = new SimpleDateFormat("yyyyMMdd")
    val date=format.format(cal.getTime)
    format.applyPattern("yyyy-MM-dd")
    val logDate=format.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val prevLogDate=format.format(cal.getTime)

    val logPath= s"/ge/gamelogs/nikkithai/$date/loggame/ntlog.log-*"
    val filterLoginLogout = (line: Array[String]) => {
      var rs = false
      if (line.length >= 25 && line(2).startsWith(logDate) &&
        (line(0) == "PlayerLogin" || line(0) == "PlayerLogout")) {
        rs = true
      }
      rs
    }
    val rawDs =spark.read.text(logPath);
    val aDs = rawDs.map(line=>line.getString(0).split("\\|")).filter(line => filterLoginLogout(line)).map(row=>{
      val id = row(6)
      val serverId = row(1)
      val dateTime = row(2)
      val stime = dateTime.replace(logDate,"")
      val titems = stime.split(":")
      var hh="00";
      if(titems.length==3){
        hh=titems.apply(0).trim
      }
      var action = ""
      if (row(0) == "PlayerLogin") {
        action = "login"
      } else {
        action = "logout"
      }
      ("nikkithai", dateTime, hh.toInt+1,serverId, action, id)
    }).toDF("game_code","log_date","hour","sid","action","aid").sort(col("aid"),col("log_date").asc).dropDuplicates("aid")

    val totalDs= spark.read.parquet(s"/ge/fairy/warehouse/nikkithai/ub/data/total_login_acc_2/$prevLogDate")
    aDs.createOrReplaceTempView("active")
    val activeByHour = spark.sql("select hour,count(distinct(aid)) as Active_by_hour  from active group by hour order by hour")
    activeByHour.createOrReplaceTempView("activeByHour")
    totalDs.createOrReplaceTempView("total")
    val newDs = spark.sql("select active.*, total.id from active LEFT OUTER JOIN total on active.aid=total.id").where("id is null")
    newDs.createOrReplaceTempView("newDs")
    val newByHour = spark.sql("select hour,count(distinct(aid)) as NRU_by_hour  from newDs group by hour order by hour")

    newByHour.createOrReplaceTempView("newByHour")

    val payTotalDs= spark.read.parquet(s"/ge/fairy/warehouse/nikkithai/ub/data/total_paid_acc_2/$prevLogDate")
    val logChargePath = s"/ge/gamelogs/nikkithai/$date/chargelog_minly/Nikki_Logcharge*.csv"
    val trimQuote = (s: String) => {
      s.replace("\"", "")
    }

    val firstFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 17 && line(11).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val getOs = (s: String) => {
      var rs = "other"
      if (s == "0") {
        rs = "ios"
      } else if (s == "1") {
        rs = "android"
      }
      rs
    }
    val convertMap = Map(
      "com.pg2.nikkithai.diamond38" -> 35,
      "com.pg2.nikkithai.diamond188" -> 100,
      "com.pg2.nikkithai.diamond377" -> 200,
      "com.pg2.nikkithai.diamond578" -> 300,
      "com.pg2.nikkithai.diamond968" -> 500,
      "com.pg2.nikkithai.diamond1968" -> 1000,
      "com.pg2.nikkithai.diamond4188" -> 2000,
      "com.pg2.nikkithai.gift" -> 30,
      "com.pg2.nikkithai.diamond90" -> 50

      )
    val _convertRate = 650

    val getGross = (s: String) => {
      val ss = s.split("-")
      var vnd: Double = 0
      if (ss.length >= 5) {
        val itemId = ss(4)
        var bac: Double = 0
        if (convertMap.contains(itemId)) {
          bac = convertMap(itemId)
        }
        vnd = bac * _convertRate
      }
      vnd
    }

    val rawPayDs =spark.read.text(logChargePath);
    val pDs = rawPayDs.map(line=>line.getString(0).split("\",\"")).filter(line => firstFilter(line)).map(row=>{
      val trans = trimQuote(row(2))
      val id = trimQuote(row(3))
      val money = row(5)
      val dateTime = row(11)
      val netRev = money.toInt * _convertRate
      val grossRev = getGross(trimQuote(row(0)))
      val stime = dateTime.replace(logDate,"")
      val titems = stime.split(":")
      var hh="00"
      if(titems.length==3){
        hh=titems.apply(0).trim
      }
      ("nikkithai", dateTime, hh.toInt+1,id, netRev, grossRev)
    }).toDF("game_code","log_date","hour","aid","net_amt","gross_amt")
    val payDs =  pDs.sort(col("aid"),col("log_date").asc).dropDuplicates("aid")
    payDs.createOrReplaceTempView("payDs")
    pDs.createOrReplaceTempView("pDs")
    payTotalDs.createOrReplaceTempView("payTotalDs")


    val firstPayDs = spark.sql("select payDs.*, payTotalDs.id from payDs LEFT OUTER JOIN payTotalDs on payDs.aid=payTotalDs.id").where("id is null")
    firstPayDs.createOrReplaceTempView("firstPayDs")
    val firstPayByHour = spark.sql("select hour,count(distinct(aid)) as NPU_by_hour, sum(gross_amt) as Gross_by_hour  from firstPayDs group by hour order by hour")
    firstPayByHour.createOrReplaceTempView("firstPayByHour")
    val payByHour = spark.sql("select hour,count(distinct(aid)) as PU_by_hour, sum(gross_amt) as Gross_by_hour  from pDs group by hour order by hour")
    payByHour.createOrReplaceTempView("payByHour")
  }
  def calcHour(active:DataFrame,pay:DataFrame,totalLogin:DataFrame,totalPay:DataFrame): Unit ={
    val minActive = active.sort(col("id"),col("log_date").asc).dropDuplicates("id")
    val minPay = pay.sort(col("id"),col("log_date").asc).dropDuplicates("id")
    val activeByHour = minActive.groupBy("hour").agg(countDistinct("id").as("active"))
    val payByHour = minPay.groupBy("hour").agg(countDistinct("id").as("pu"))

  }
  def getAf(): Unit ={
    val spark = SparkEnv.getSparkSession
    import spark.implicits._
    val ds = spark.read.parquet("/ge/fairy/warehouse/appsflyer/install5/2016-*").withColumn("month",substring(col("install_time"),0,7)).where("month like '2016-%'")
    val rs = ds.groupBy("month","app_id","media_source").agg(countDistinct("appsflyer_device_id").as("install")).sort("month","app_id")
    rs.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/af-2016/install-2016.csv")
  }
}