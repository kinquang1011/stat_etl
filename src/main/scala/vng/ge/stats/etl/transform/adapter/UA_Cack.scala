package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.transform.udf.MyUdf.dateTimeIncrement
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils}

import scala.util.Try

/**
  * Created by quangctn on 08/02/2017.
  */
class UA_Cack extends MyplayFormatter ("appsflyer_cack") {

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
    createData(logDate,"",0)

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

  def createData(logDate: String, hourly: String,rangeDate: Int): Unit = {
    val spark = SparkEnv.getSparkSession
    val date=logDate
    val pushDs= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4/*").where(s"app_id in ('com.vng.cuuam','id1089824208')").where(s"logdate<='$date'")
    val changeDateTime = udf {(datetime : String) =>
      val newStr = dateTimeIncrement(datetime,7*3600)
      //newStr.substring(0,10)
      newStr
    }
    val pullDs= spark.read.option("header","true").csv("/ge/gamelogs/appsflyer_api/2016_0*/cack*").withColumn("n_install_time",changeDateTime(col("Install Time"))).withColumn("n_android_id", upper(col("Android ID"))).withColumn("n_ad_id", upper(col("Advertising ID"))).select("n_install_time","Event Name","App ID","AppsFlyer ID","Media Source","Platform", "n_install_time","IDFA","n_android_id","n_ad_id","Campaign").toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform", "install_time", "idfa", "android_id", "advertising_id","campaign").where("install_time>'2016-06-14'")
    val ppDs = pushDs.union(pullDs)
    val openAppDs = getDataOpenApp(logDate,"")
    openAppDs.createOrReplaceTempView("openAppDs")
    ppDs.createOrReplaceTempView("ppDs")
    val openAppNotIn = sparkSession.sql("select ppDs.*,openAppDs.appsflyer_device_id as api from  ppDs left outer join openAppDs on ppDs.appsflyer_device_id=openAppDs.appsflyer_device_id").where("api is null").drop("api")
    val allDs = ppDs.union(openAppNotIn)
    storeInParquet(allDs,Constants.WAREHOUSE_DIR + s"/cack/ub/sdk_data/appsflyer/total_install/$logDate")

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

    val srcReg =iosReg.union(and1Reg).union(and2Reg).union(appReg).distinct()
    storeInParquet(srcReg,Constants.WAREHOUSE_DIR + s"/cack/ub/sdk_data/appsflyer/new_register/$logDate")


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
    import sparkSession.implicits._
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


  def checkOpenApp(logDate: String, hourly: String): DataFrame = {
    import sparkSession.implicits._
    val spark = SparkEnv.getSparkSession
    val date ="2016-06-28"

    val df = spark.read.json(s"/ge/gamelogs/sdk/$date/CACK_Login_OpenApp/CACK_Login_OpenApp-$date.gz").withColumn("did",upper(regexp_replace(col("device_id"), "_", "-")))


    val finDs = df.map(row=>{
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

      (date,"open_app",app_id,appFlyer_id, media_source,device_os,updatetime,device_id,android_id,advertising_id,campain)
    }).toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform",
      "install_time", "idfa", "android_id", "advertising_id","campaign")

    val rs = finDs.sort("appsflyer_device_id").dropDuplicates("appsflyer_device_id")
    rs
  }

  def check(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession

    val pushDs= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4/*").where(s"app_id in ('com.vng.cuuam','id1089824208')").where("logdate<='2016-07-01'")

    val pullDs= spark.read.option("header","true").csv("/ge/gamelogs/appsflyer_api/2016_0*/cack*").withColumn("n_install_time", col("Install Time")).withColumn("n_android_id", upper(col("Android ID"))).withColumn("n_ad_id", upper(col("Advertising ID"))).select("n_install_time","Event Name","App ID","AppsFlyer ID","Media Source","Platform", "n_install_time","IDFA","n_android_id","n_ad_id","Campaign").toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform", "install_time", "idfa", "android_id", "advertising_id","campaign")
    val allDs = pushDs.union(pullDs).sort("appsflyer_device_id","idfa","android_id").dropDuplicates("appsflyer_device_id","idfa","android_id")

    val date="2016-07-01"
    val totalIns = spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_data/appsflyer/total_install/$date")
    totalIns.createOrReplaceTempView("totalIns")

    val newreg =  spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_ua/accregister_2/$date")
    val login = spark.read.json(s"/ge/gamelogs/sdk/$date/CACK_Login_InfoLog/CACK_Login_InfoLog-$date.gz").select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("advertising_id",upper(col("advertising_id"))).withColumn("device_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("android_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
    newreg.createOrReplaceTempView("newreg")
    login.createOrReplaceTempView("login")
    val newRegSrc = spark.sql("select login.* from login, newreg where newreg.id = login.userID")
    newRegSrc.createOrReplaceTempView("newRegSrc")

    //allDs.createOrReplaceTempView("allDs")
    val iosReg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform, totalIns.install_time,newRegSrc.userID as user_id from totalIns, newRegSrc where totalIns.idfa = newRegSrc.device_id")


    val and1Reg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform, totalIns.install_time,newRegSrc.userID as user_id from totalIns, newRegSrc where totalIns.android_id = newRegSrc.android_id")
    //srcreg_and1.select("userID")

    val and2Reg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform, totalIns.install_time,newRegSrc.userID as user_id from totalIns, newRegSrc where totalIns.advertising_id = newRegSrc.advertising_id")

    val appReg = spark.sql("select totalIns.appsflyer_device_id,totalIns.media_source,totalIns.platform, totalIns.install_time,newRegSrc.userID as user_id from totalIns, newRegSrc where totalIns.appsflyer_device_id = newRegSrc.appFlyer_id")

    //srcreg_and2.select("userID")

    val srcReg =iosReg.union(and1Reg).union(and2Reg).union(appReg).distinct()

    srcReg.createOrReplaceTempView("srcReg")
    spark.sql("select platform, count(distinct(user_id)) as nru from srcReg group by platform").show(false)


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
    totalIns.where("android_id='' and idfa=''").show()
  }

  def checkRs(logDate: String, hourly: String): Unit = {
    val spark = SparkEnv.getSparkSession
    val fmedia  = (media:String)=> {
      var result = "Others";
      if (media != null) {
        result = media.trim.replace(" ", "").toLowerCase

      }
      result
    }
    val mediaf = udf(fmedia)
    val date ="2016-06-29"
    val reg=spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_data/appsflyer/new_register/$date").withColumn("src",mediaf(col("media_source"))).withColumn("d1",col("install_time").substr(0,10)).withColumn("d2",col("reg_date").substr(0,10))
    reg.createOrReplaceTempView("reg")
    val mapSrcPath="tmp/map-src-appflyer.csv"
    val srcDs = spark.read.option("delimiter",",").option("header","true").csv(mapSrcPath).withColumn("src",mediaf(col("media_source"))).sort("src").dropDuplicates("src")
    srcDs.createOrReplaceTempView("srcDs")
    val mapDs = spark.sql("select reg.*,srcDs.source as source from reg LEFT OUTER JOIN srcDs on reg.src=srcDs.src")
    mapDs.createOrReplaceTempView("mapDs")

    spark.sql("select platform, count(distinct(user_id)) as nru from mapDs group by platform").show(false)

    val totalIns = spark.read.parquet(s"/ge/warehouse/cack/ub/sdk_data/appsflyer/total_install/$date")
    totalIns.createOrReplaceTempView("totalIns")

    //rs.createOrReplaceTempView("rs")

    spark.sql("select source,platform, count(distinct(user_id)) as nru from rs left outer jo group by source,platform").show(false)
  }
}