package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.transform.udf.MyUdf.dateTimeIncrement
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils}

import scala.util.Try

/**
  * Created by quangctn on 08/02/2017.
  */
class Appsflyer_Cack extends MyplayFormatter ("appsflyer_cack") {

  val emptyTotalInstall: DataFrame = sqlContext.createDataFrame(sparkContext.emptyRDD[Row],
    StructType(StructField("logdate", StringType, nullable = true)
      :: StructField("event_type", StringType, nullable = true)
      :: StructField("app_id", StringType, nullable = true)
      :: StructField("appsflyer_device_id", StringType, nullable = true)
      :: StructField("media_source", StringType, nullable = true)
      :: StructField("platform", StringType, nullable = true)
      :: StructField("install_time", StringType, nullable = true)
      :: StructField("idfa", StringType, nullable = true)
      :: StructField("android_id", StringType, nullable = true)
      :: StructField("advertising_id", StringType, nullable = true)
      :: StructField("campaign", StringType, nullable = true)
      //:: StructField("net_amt", DoubleType, nullable = true)
      :: Nil))

  def start(args: Array[String]): Unit = {
    initParameters(args)
    var logDate: String = ""
    for (x <- args) {
      val xx = x.split("=")
      if (xx(0).contains("logDate")) {
        logDate = xx(1)
      }
    }
    this -> otherETL(logDate,"","") -> close
  }

  //Generate parquet file to folder ge/fairy/warehouse
  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
    val ds = createData(logDate,"",0)
    /*val outputPath = Constants.WAREHOUSE_DIR + s"/cack/ub/sdk_data/appsflyer/solu1/$logDate"*/
    //search log 3 day and filter it base on install da
    val outputPath = Constants.WAREHOUSE_DIR + s"/cack/ub/sdk_data/appsflyer/solu4/$logDate"
    storeInParquet(ds,outputPath)
  }
  def getTotalInstallLog(pathList: Array[String]): DataFrame = {
    Common.logger("getParquetLog, path = " + pathList.mkString(","))
    var rs: DataFrame = emptyTotalInstall
    Try {
      rs = sqlContext.read.parquet(pathList: _*)
    }
    rs
  }

  def getTotalInstallLog(path: String): DataFrame = {
    Common.logger("getParquetLog, path = " + path)
    var rs: DataFrame = emptyTotalInstall
    Try {
      rs = sqlContext.read.parquet(path)
    }
    rs
  }
  def createData(logDate: String, hourly: String,rangeDate: Int): DataFrame = {
    //var CackDs: DataFrame = emptyDataFrame

    def patternAf (ldate: String):String = {
      Constants.FAIRY_WAREHOUSE_DIR + s"/appsflyer/install4/$ldate/*"
    }
    var arr: Array[String] = Array()
    var path: Array[String] = Array()
    if (rangeDate == 0){
      path = Array(patternAf(logDate))
    }
    else{
      for (i<-(-1*rangeDate) to rangeDate) {
        val date = DateTimeUtils.getDateDifferent(i, logDate, Constants.TIMING, Constants.A1)
        path = path ++ Array(patternAf(date))
      }
    }
    Common.logger("path = " + path.mkString(","))
    val pushDs = getParquetLog(path).where(s"app_id in ('com.vng.cuuam','id1089824208') and install_time like '$logDate%'")
    //storeInParquet(pushDs,s"tmp/pushDs/$logDate")
    val apiCallDs = getDataFromAPICall(logDate,"")
    val openAppDs = getDataOpenApp(logDate,"")
    apiCallDs.createOrReplaceTempView("apiCallDs")
    pushDs.createOrReplaceTempView("pushDs")
    openAppDs.createOrReplaceTempView("openAppDs")


    val apiNotInPush = sparkSession.sql("select pushDs.*,apiCallDs.appsflyer_device_id as api from  pushDs left outer join apiCallDs on pushDs.appsflyer_device_id=apiCallDs.appsflyer_device_id").where("api is null").drop("api")

    val openAppNotInPush = sparkSession.sql("select pushDs.*,openAppDs.appsflyer_device_id as api from  pushDs left outer join openAppDs on pushDs.appsflyer_device_id=openAppDs.appsflyer_device_id").where("api is null").drop("api")

    val prevDate = vng.ge.stats.report.util.DateTimeUtils.getPreviousDate(logDate, "a1")
    val totalPrevDF = getTotalInstallLog(Constants.WAREHOUSE_DIR + s"/cack/ub/sdk_data/appsflyer/total_install/$prevDate")
    val dailyInstall = pushDs.union(apiNotInPush).union(openAppNotInPush).withColumn("idfa",upper(col("idfa"))).withColumn("android_id",upper(col("android_id"))).distinct()

    val dailyTotalInstall = dailyInstall.union(totalPrevDF)
    storeInParquet(dailyTotalInstall,Constants.WAREHOUSE_DIR + s"/cack/ub/sdk_data/appsflyer/total_install/$logDate")


    //val appsflyer =exDs.withColumn("idfa",upper(col("idfa"))).withColumn("android_id",upper(col("android_id")))
    dailyTotalInstall.createOrReplaceTempView("appsflyer")

    val sdkLoginPath = s"/ge/gamelogs/sdk/$logDate/CACK_Login_InfoLog/CACK_Login_InfoLog-$logDate.gz"
    val sdk = sparkSession.read.json(sdkLoginPath).select("appFlyer_id","device_id","userID","android_id","advertising_id").withColumn("device_id",upper(regexp_replace(col("device_id"), "_", "-"))).withColumn("android_id",upper(col("android_id"))).withColumn("advertising_id",upper(col("advertising_id"))).sort("userID").dropDuplicates("userID")

    sdk.createOrReplaceTempView("sdk")

    val iosdf = sparkSession.sql("select appsflyer.appsflyer_device_id, appsflyer.install_time, appsflyer.media_source," +
      "appsflyer.campaign, appsflyer.platform, appsflyer.idfa, appsflyer.android_id, sdk.userID from appsflyer " +
      "left join sdk on appsflyer.idfa=sdk.device_id")

    val androiddf = sparkSession.sql("select appsflyer.appsflyer_device_id, appsflyer.install_time,appsflyer.media_source," +
      "appsflyer.campaign,appsflyer.platform,appsflyer.idfa, appsflyer.android_id, sdk.userID from appsflyer " +
      "left join sdk on appsflyer.android_id=sdk.android_id")

    val androiddf2 = sparkSession.sql("select appsflyer.appsflyer_device_id, appsflyer.install_time,appsflyer.media_source," +
      "appsflyer.campaign,appsflyer.platform,appsflyer.idfa, appsflyer.android_id, sdk.userID from appsflyer " +
      "join sdk on appsflyer.advertising_id=sdk.advertising_id and appsflyer.platform='android'")


    iosdf.union(androiddf).union(androiddf2).sort("userID").dropDuplicates("userID")
  }

  def getDataFromAPICall(logDate: String, hourly: String): DataFrame = {
    val logdte = logDate.substring(0,7).replace("-","_")
    var ds = sparkSession.read.format("csv").option("header","true").load(s"/tmp/appsflyer/appsflyer/cack_*.csv").dropDuplicates()

    val changeDateTime = udf {(datetime : String) =>
      val newStr = dateTimeIncrement(datetime,7*3600)
      //newStr.substring(0,10)
      newStr
    }

    ds = ds.withColumn("n_install_time", changeDateTime(col("Install Time")))
    ds = ds.withColumn("n_android_id", upper(col("Android ID")))
    ds = ds.withColumn("n_ad_id", upper(col("Advertising ID")))
    ds = ds.select("n_install_time","Event Name","App ID","AppsFlyer ID","Media Source","Platform",
      "n_install_time","IDFA","n_android_id","n_ad_id","Campaign")
      .toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform",
        "install_time", "idfa", "android_id", "advertising_id","campaign")
    val fds = ds.where(s"n_install_time like '$logDate%'")
    fds
  }


  def getDataOpenApp(logDate: String, hourly: String): DataFrame = {
    import  sparkSession.implicits._
    val patternPath = Constants.GAMELOG_DIR + "/sdk/[yyyy-MM-dd]/CACK_Login_OpenApp/CACK_Login_OpenApp-[yyyy-MM-dd].gz"

    val path = PathUtils.generateLogPathDaily(patternPath,logDate,2)
    val df = getJsonLog(path).withColumn("did",upper(regexp_replace(col("device_id"), "_", "-"))).withColumn("dfill",col("updatetime").substr(0,10))
    val datedf = df.where(s"dfill='$logDate'")

    val finDs = datedf.map(row=>{
      val referrer = row.getAs[String]("referrer")


      val app_id = row.getAs[String]("package_name")
      var android_id = row.getAs[String]("os_id")
      if(android_id==null){
        android_id=""
      }
      var device_id = row.getAs[String]("did")
      if(device_id==null){
        device_id=""
      }
      val device_os = row.getAs[String]("device_os").toLowerCase
      if(device_os.equalsIgnoreCase("ios")){
        android_id=""
      }else if(device_os.equalsIgnoreCase("android")){
        device_id=""
      }

      val appFlyer_id = row.getAs[String]("appFlyer_id")

      val advertising_id = row.getAs[String]("advertising_id")
      val updatetime = row.getAs[String]("updatetime")

      var media_source = "others"
      var campain = "others"
      if(referrer!=null){
        val eurl = java.net.URLDecoder.decode(referrer, "UTF-8");
        val params:Array[String] = eurl.split("&")
        for (x <- params){
          val kv:Array[String] = x.split("=")
          if(kv.length==2 && kv.apply(0).equalsIgnoreCase("utm_medium")){
            if(!kv.apply(1).contains("(not")){
              media_source = kv.apply(1)
            }
          }
          if(kv.length==2 && kv.apply(0).equalsIgnoreCase("pid")){
            media_source = kv.apply(1)
          }
          if(kv.length==2 && kv.apply(0).equalsIgnoreCase("c")){
            campain = kv.apply(1)
          }
        }
      }
      device_id=device_id.toUpperCase
      android_id=android_id.toUpperCase

      (logDate,"open_app",app_id,appFlyer_id, media_source,device_os,updatetime,device_id,android_id,advertising_id,campain)
    }).toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform",
      "install_time", "idfa", "android_id", "advertising_id","campaign")

    finDs.sort("appsflyer_device_id").dropDuplicates("appsflyer_device_id")

  }
  def check(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession
    val date="2016-06-28"
    val pushDs= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4/*").where(s"app_id in ('com.vng.cuuam','id1089824208')").where(s"logdate<='$date'")

    val pullDs= spark.read.option("header","true").csv("/ge/gamelogs/appsflyer_api/2016_0*/cack*").withColumn("n_install_time", col("Install Time")).withColumn("n_android_id", upper(col("Android ID"))).withColumn("n_ad_id", upper(col("Advertising ID"))).select("n_install_time","Event Name","App ID","AppsFlyer ID","Media Source","Platform", "n_install_time","IDFA","n_android_id","n_ad_id","Campaign").toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform", "install_time", "idfa", "android_id", "advertising_id","campaign").where("install_time>'2016-06-27'")
    val allDs = pushDs.union(pullDs)
      //.sort("appsflyer_device_id","idfa","android_id").dropDuplicates("appsflyer_device_id","idfa","android_id")


    //val totalIns = spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_data/appsflyer/total_install/$date")
    //totalIns.createOrReplaceTempView("totalIns")

    val newreg =  spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_ua/accregister_2/$date")
    val login = spark.read.json(s"/ge/gamelogs/sdk/$date/CACK_Login_InfoLog/CACK_Login_InfoLog-$date.gz").select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("advertising_id",upper(col("advertising_id"))).withColumn("device_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("android_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
    newreg.createOrReplaceTempView("newreg")
    login.createOrReplaceTempView("login")
    val newRegSrc = spark.sql("select login.*,newreg.log_date as reg_date from login, newreg where newreg.id = login.userID")
    newRegSrc.createOrReplaceTempView("newRegSrc")

    allDs.createOrReplaceTempView("totalIns")
    val iosReg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform,totalIns.campaign, totalIns.install_time,newRegSrc.userID as user_id,newRegSrc.reg_date from totalIns, newRegSrc where totalIns.idfa = newRegSrc.device_id")


    val and1Reg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform,totalIns.campaign, totalIns.install_time,newRegSrc.userID as user_id,newRegSrc.reg_date from totalIns, newRegSrc where totalIns.android_id = newRegSrc.android_id")
    //srcreg_and1.select("userID")

    val and2Reg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform, totalIns.campaign,totalIns.install_time,newRegSrc.userID as user_id,newRegSrc.reg_date from totalIns, newRegSrc where totalIns.advertising_id = newRegSrc.advertising_id")

    val appReg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform,totalIns.campaign, totalIns.install_time,newRegSrc.userID as user_id,newRegSrc.reg_date from totalIns, newRegSrc where totalIns.appsflyer_device_id = newRegSrc.appFlyer_id")

    //srcreg_and2.select("userID")

    val srcReg =iosReg.union(and1Reg).union(and2Reg).union(appReg).distinct()

    srcReg.createOrReplaceTempView("srcReg")
    spark.sql("select media_source,platform, count(distinct(user_id)) as nru from srcReg group by media_source,platform").show(false)


    val nru2 = spark.sql("select totalIns.*,newRegSrc.userID from totalIns, newRegSrc where totalIns.appsflyer_device_id = newRegSrc.appFlyer_id")
    nru2.select("userID").distinct().count()



  }
  def checkInstall(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession
    val pushDs= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4/*").where(s"app_id in ('com.vng.cuuam','id1089824208')")

    val rawapp = spark.read.option("delimiter","\t").csv("/ge/gamelogs/appsflyer/20161017/appsflyer.20161017.log.gz")
    var date="2016-10-17"
    val login = spark.read.json(s"/ge/gamelogs/sdk/$date/CACK_Login_InfoLog/CACK_Login_InfoLog-$date.gz").select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("advertising_id",upper(col("advertising_id"))).withColumn("device_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("android_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
    login.where("appFlyer_id='1476678612000-9738640'").show()
    date="2016-06-28"
    val totalIns = spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_data/appsflyer/total_install/$date")

    val sdk =  spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_ua/accregister_2/$date")
    val newreg =  spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_data/appsflyer/new_register/$date")
    totalIns.createOrReplaceTempView("totalIns")
    newreg.createOrReplaceTempView("newRegSrc")
    val xch= spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform,totalIns.campaign, totalIns.install_time,newRegSrc.user_id as user_id,newRegSrc.reg_date from totalIns, newRegSrc where totalIns.appsflyer_device_id = newRegSrc.appsflyer_device_id")

    totalIns.where("android_id='' and idfa=''").show()
  }
}