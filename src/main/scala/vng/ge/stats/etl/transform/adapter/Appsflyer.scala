package vng.ge.stats.etl.transform.adapter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils}
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.transform.udf.MyUdf._


/**
  * Created by quangctn on 08/02/2017.
  */
class Appsflyer extends MyplayFormatter("appsflyer") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    var logDate: String = ""
    for (x <- args) {
      val xx = x.split("=")
      if (xx(0).contains("logDate")) {
        logDate = xx(1)
      }
    }
    this -> otherETL(logDate, "", "") -> close
  }

  //Generate parquet file to folder ge/fairy/warehouse
  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
    val dsAppsFlyerPush = getDataAppsFlyerPush(logDate, "")
    val outputPath = Constants.FAIRY_WAREHOUSE_DIR + s"/appsflyer/install5" +
      s"/$logDate"
    storeInParquet(dsAppsFlyerPush, outputPath)
    val gs2Apps = getDataGs2(logDate, "")
    val outputGsPath = Constants.FAIRY_WAREHOUSE_DIR + s"/appsflyer/gs2/install/" +
      s"/$logDate"
    storeInParquet(gs2Apps, outputGsPath)

  }

  def getData(logDate: String, hourly: String): DataFrame = {
    var afRaw: RDD[String] = null
    val lDate: String = logDate.replace("-", "")
    val patternAf = Constants.GAMELOG_DIR + s"/appsflyer/$lDate/*"
    val path = PathUtils.generateLogPathDaily(patternAf, logDate)
    Common.logger("path = " + path.mkString(","))
    afRaw = getRawLog(path)
    val trimQuote = (s: String) => {
      s.replace("\"", "")
    }
    val getValue = (line: Array[String], columnName: String) => {
      var re = new StringBuilder()
      for (x <- line) {
        val length = x.split(":").length
        if (length >= 2) {
          if (trimQuote(x.split(":")(0)).equalsIgnoreCase(columnName)) {
            if (length == 2) {
              re = re.append(trimQuote(x.split(":")(1)))
            } else {
              if (length == 4 && columnName.contains("time")) {
                re = re.append(trimQuote(x.split(":")(1))).append(":")
                  .append(trimQuote(x.split(":")(2))).append(":")
                  .append(trimQuote(x.split(":")(3)))
              }
            }
          }
        }
      }
      re.toString()
    }
    val rddAf2 = afRaw.map(line => line.split("\t")).map { line =>
      val abc = line(2).substring(1, line(2).length - 1)
      (abc)
    }
    val afDs = rddAf2.map(line => line.split(",")).map { line =>
      val appId = getValue(line, "app_id")
      val appDeviceId = getValue(line, "appsflyer_device_id")
      val name = getValue(line, "bundle_id")
      val media = getValue(line, "media_source")
      val campaign = getValue(line, "campaign")
      val fb_adgroup_id = getValue(line, "fb_adgroup_id")
      val af_channel = getValue(line, "af_channel")
      val imei = getValue(line, "imei")
      val platform = getValue(line, "platform")
      val download_time = dateTimeIncrement(getValue(line, "download_time"), 7 * 3600)
      val install_time = dateTimeIncrement(getValue(line, "install_time"), 7 * 3600)
      val country_code = getValue(line, "country_code")
      val idfa = getValue(line, "idfa")
      val click_time = getValue(line, "click_time")
      val af_sub1 = getValue(line, "af_sub1")
      val device_type = getValue(line, "device_type")
      val device_name = getValue(line, "device_name")
      val os_version = getValue(line, "os_version")
      val idfv = getValue(line, "idfv")
      val event_type = getValue(line, "event_type")
      val android_id = getValue(line, "android_id")
      (appId, appDeviceId, name, media, campaign, fb_adgroup_id, af_channel, imei, platform
        , download_time, install_time, country_code, logDate, idfa, click_time, af_sub1
        , device_type, device_name, os_version, idfv, event_type, android_id
      )
    }.toDF("app_id", "appsflyer_device_id", "bundle_id", "media_source", "campaign"
      , "fb_adgroup_id", "af_channel", "imei", "platform"
      , "download_time", "install_time", "country_code", "logDate", "idfa", "click_time"
      , "af_sub1", "device_type", "device_name", "os_version", "idfv", "event_type"
      , "android_id")
    afDs
  }

  def getDataAppsFlyerPush(logDate: String, hourly: String): DataFrame = {
    var afRaw: RDD[String] = null
    val lDate: String = logDate.replace("-", "")
    val patternAf = Constants.GAMELOG_DIR + s"/appsflyer/$lDate/*"
    val path = PathUtils.generateLogPathDaily(patternAf, logDate, 1)
    Common.logger("path = " + path.mkString(","))
    afRaw = getRawLog(path)
    val trimQuote = (s: String) => {
      s.replace("\"", "")
    }
    val getValue = (line: Array[String], columnName: String, upperFlg: Boolean) => {
      var value = ""
      var re = new StringBuilder()
      for (x <- line) {
        val length = x.split(":").length
        if (length >= 2) {
          if (trimQuote(x.split(":")(0)).equalsIgnoreCase(columnName)) {
            if (length == 2) {
              re = re.append(trimQuote(x.split(":")(1)))
            } else {
              if (length == 4 && columnName.contains("time")) {
                re = re.append(trimQuote(x.split(":")(1))).append(":")
                  .append(trimQuote(x.split(":")(2))).append(":")
                  .append(trimQuote(x.split(":")(3)))
              }
            }
          }
        }
      }
      if (upperFlg) {
        value = re.toString().toUpperCase
      } else {
        value = re.toString()
      }
      if (value.equalsIgnoreCase("null")) {
        value = ""
      }
      value
    }
    val rddAf2 = afRaw.map(line => line.split("\t")).map { line =>
      val abc = line(2).substring(1, line(2).length - 1)
      (abc)
    }
    val afDs = rddAf2.map(line => line.split(",")).map { line =>
      val event_type = getValue(line, "event_type", false)
      val appId = getValue(line, "app_id", false)
      val appDeviceId = getValue(line, "appsflyer_device_id", false)
      val media = getValue(line, "media_source", false)
      val platform = getValue(line, "platform", false)
      val install_time = dateTimeIncrement(getValue(line, "install_time", false), 7 * 3600)
      val idfa = getValue(line, "idfa", true)
      val android_id = getValue(line, "android_id", true)
      val advertising_id = getValue(line, "advertising_id", true)
      val campaign = getValue(line, "campaign", false)
      val attribution_type = getValue(line, "attribution_type", false)
      val imei = getValue(line, "imei", false)
      val app_name = getValue(line, "app_name", false)

      (logDate, event_type, appId, appDeviceId, media, platform, install_time, idfa, android_id, advertising_id
        , campaign, attribution_type, imei, app_name
      )
    }.toDF("logdate", "event_type", "app_id", "appsflyer_device_id", "media_source", "platform",
      "install_time", "idfa", "android_id", "advertising_id", "campaign", "attribution_type", "imei", "app_name")
    afDs
  }

  //gs2
  def getDataGs2(logDate: String, hourly: String): DataFrame = {
    val lDate: String = logDate.replace("-", "")
    val patternAf = Constants.GAMELOG_DIR + s"/appsflyer_api/gs2/3-day-ago/$lDate/{installs,organic_installs}*"
    val sf = Constants.FIELD_NAME
    var appDf = getCsvWithHeaderLog(Array(patternAf), ",").withColumn(sf.LOG_DATE, lit(logDate))
      .select(
        sf.LOG_DATE, "Event Name", "App ID", "AppsFlyer ID", "Media Source", "Platform",
        "Install Time", "IDFA", "Android ID", "Advertising ID", "Campaign", "Is Primary Attribution", "IMEI", "App Name"
      )
    appDf = appDf.toDF("logdate", "event_type", "app_id", "appsflyer_device_id", "media_source", "platform",
      "install_time", "idfa", "android_id", "advertising_id", "campaign", "attribution_type", "imei", "app_name")
  appDf
  }


  def getResult(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions.sum
    val afPath = "/ge/fairy/warehouse/appsflyer/install/2016-07-*"
    val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").select("appsflyer_device_id", "media_source").distinct().sort("appsflyer_device_id").dropDuplicates("appsflyer_device_id")

    val sdkLoginPath = "/ge/gamelogs/sdk/2017-02-*/CACK_Login_InfoLog-2017-02*.gz"
    val sdkLoginDs = spark.read.json(sdkLoginPath).select("appFlyer_id", "imei", "userID", "advertising_id", "package_name").sort("userID").dropDuplicates("userID")

    val sdkDBGPath = "/ge/gamelogs/sdk/2017-02-*/Log_CACK_DBGAdd-2017-02-*.gz"
    val sdkDBGDs = spark.read.json(sdkDBGPath).select("userID", "pmcNetChargeAmt").groupBy("userID").agg(sum("pmcNetChargeAmt").alias("pmcNetChargeAmt"))
    sdkDBGDs.createOrReplaceTempView("sdkDBGDs")
    sdkLoginDs.createOrReplaceTempView("sdkLoginDs")
    val sdkDs = spark.sql("select sdkLoginDs.userID,sdkLoginDs.appFlyer_id,sdkDBGDs.pmcNetChargeAmt from sdkLoginDs LEFT OUTER JOIN sdkDBGDs on sdkLoginDs.userID=sdkDBGDs.userID")
    sdkDs.createOrReplaceTempView("sdkDs")
    afDs.createOrReplaceTempView("afDs")
    val mDs = spark.sql("select sdkDs.userID,sdkDs.appFlyer_id,sdkDs.pmcNetChargeAmt,afDs.media_source from sdkDs LEFT OUTER JOIN afDs on sdkDs.appFlyer_id=afDs.appsflyer_device_id")
    mDs.select("*").where("media_source!=null").show(false)
    mDs.coalesce(1).write.mode("overwrite").format("csv").save("tmp/appflyer.csv")
  }

  /*def getDataFull(sparkContext:SparkContext, path: String): DataFrame = {
    var afRaw: RDD[String] = null
    afRaw = sparkContext.textFile(path)
    val trimQuote = (s: String) => {
      s.replace("\"", "")
    }
    val getValue =(line : Array[String], columnName: String) => {
      var re = ""
      for (x <- line){
        if(x.split(":").length>=2) {
          if (trimQuote(x.split(":")(0)).equalsIgnoreCase(columnName)) {
            re = trimQuote(x.split(":")(1))
          }
        }
      }
      re
    }
    val rddAf2 = afRaw.map(line => line.split ("\t")).map {line =>
      val abc = line (2)
      (abc)}

    val afDs = rddAf2.map(line => line.split(",")).map { line =>
      val appId = getValue(line,"app_id")
      val appDeviceId = getValue(line,"appsflyer_device_id")
      val name = getValue(line,"bundle_id")
      val media =  getValue(line,"media_source")
      val campaign = getValue(line,"campaign")
      val fb_adgroup_id = getValue(line,"fb_adgroup_id")
      val af_channel = getValue(line,"af_channel")
      val imei = getValue(line,"imei")
      val platform = getValue(line,"platform")
      val download_time = getValue(line,"download_time")
      val install_time = getValue(line,"install_time")
      val country_code = getValue(line, "country_code")
      val idfa = getValue(line,"idfa")
      val click_time = getValue(line,"click_time")
      val af_sub1 = getValue(line,"af_sub1")
      val device_type = getValue(line,"device_type")
      val device_name = getValue(line,"device_name")
      val os_version = getValue(line,"os_version")
      val idfv = getValue(line,"idfv")
      val event_type = getValue(line,"event_type")
      val android_idy = getValue(line,"android_id")
      (appId, appDeviceId, name, media, campaign, fb_adgroup_id, af_channel, imei,platform
        ,download_time, install_time, country_code, idfa, click_time, af_sub1
        ,device_type, device_name,os_version,idfv, event_type, candroid_id
      )
    }.toDF("app_id","appsflyer_device_id","bundle_id","media_source","campaign"
      ,"fb_adgroup_id","af_channel","imei","platform"
      ,"download_time","install_time","country_code","idfa", "click_time"
      , "af_sub1", "device_type", "device_name", "os_version", "idfv", "event_type"
      , "android_id")
    afDs
  }*/
  def generateReportByDate(spark: SparkSession): Unit = {
    val path = "/ge/fairy/warehouse/appsflyer/install2/*"
    val fullDs = spark.read.load(path)

    val cackDs = fullDs.filter(fullDs("install_time").gt(lit("2016-07-16")))
      .where("app_id in ('com.vng.cuuam','id1089824208')")
      .select("appsflyer_device_id", "media_source", "campaign", "install_time")
      .withColumn("date", substring(col("install_time"), 0, 10))
    val resultDs = cackDs.groupBy("date", "media_source", "campaign").agg(countDistinct("appsflyer_device_id"))
    resultDs.coalesce(1).write.mode("overwrite").format("csv").save("tmp/statsAppByDate_4.csv")
  }

  def generateAF(spark: SparkSession, sc: SparkContext): DataFrame = {
    val path = "/ge/fairy/warehouse/appsflyer/install3/2016-07-0*/*.parquet"
    var ds = spark.read.load(path)
    ds = ds.withColumn("newDate", col("install_time").substr(0, 10))
    ds.groupBy("newDate").agg(countDistinct(""))
    ds
  }

  def generateQuitUser(spark: SparkSession): Unit = {
    val path102016ToNow = "/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2017-02-28"
    val dsLogin102016 = spark.read.load(path102016ToNow)

    val pathLogin3_16 = "/ge/warehouse/cack/ub/sdk_data/activity_2/2017-03*/*.parquet"
    val dsLogin3_17 = spark.read.load(pathLogin3_16)

    dsLogin102016.createOrReplaceTempView("login1")
    dsLogin3_17.createOrReplaceTempView("login2")
    val join = spark.sql("select login1.log_date, login1.id, login2.id " +
      "from login1 " +
      "left join login2 " +
      "on login1.id == login2.id")
    val quitDs = join.where("login2.id is null").select("login1.id").distinct()
    quitDs.coalesce(1).write.mode("overwrite").format("csv")
      .save("quangctn/af_cack/quit.csv")

    val allp = spark.read.load("/ge/warehouse/cack/ub/sdk_data/payment_2/*/*.parquet")
    quitDs.createOrReplaceTempView("quituid")
    allp.createOrReplaceTempView("allpayment")
    val total = spark.sql("select quituid.id, allpayment.gross_amt, allpayment.net_amt " +
      "from quituid " +
      "left join allpayment " +
      "on quituid.id==allpayment.id").where("allpayment.gross_amt is not null")
    val totalRev = total.groupBy("id").agg(sum("gross_amt"), sum("net_amt"))
    totalRev.coalesce(1).write.mode("overwrite").format("csv")
      .save("quangctn/af_cack/total_rev.csv")
    val allActivity = spark.read.load("/ge/warehouse/cack/ub/sdk_data/activity_2/*/*.parquet")
    val joinLastLogin = quitDs.as("a").join(allActivity.as("b"), quitDs("id") === allActivity("id"), "left_outer")
      .select("a.id", "b.log_date")
    val lastLogin = joinLastLogin.orderBy(desc("log_date")).dropDuplicates("id")
    lastLogin.coalesce(1).write.mode("overwrite").format("csv")
      .save("quangctn/af_cack/last_login.csv")
    val allsolu3 = spark.read.load("/ge/warehouse/cack/ub/sdk_data/appsflyer/solu3/*/*.parquet")
      .where("userID is not null and userID !='(null)'")
    val deviceIdDs = lastLogin.as("a").join(allsolu3.as("b"), lastLogin("id") === allsolu3("id"), "left_outer")
      .select("a.userID", "b.android", "b.idfa")
      .orderBy(desc("android"), desc("idfa")).dropDuplicates("userID")

    deviceIdDs.coalesce(1).write.mode("overwrite").format("csv")
      .save("quangctn/af_cack/af_user.csv")
    /*val addPrefix = udf {(str : String) =>
      val prefix = "u_"
      val newStr = prefix + str
      newStr

      ds1.as("a")
      .join(ds2.as("b"),ds1("userid")===ds2("userid"),"left_outer")
      .join(ds3.as("c"),ds1("userid")===ds3("userid"),"left_outer")
      .join(ds4.as("d"),ds1("userid")===ds4("userid"),"left_outer")
      .withColumn("nuid",addPrefix(col("a.userid")))
      .select("nuid","b.gross_amt","b.net_amt","c.logdate","d.android_id","d.idfa")
      .coalesce(1).write.mode("overwrite").format("csv")
      .save("quangctn/af_cack/cack.csv")*/

  }

}
