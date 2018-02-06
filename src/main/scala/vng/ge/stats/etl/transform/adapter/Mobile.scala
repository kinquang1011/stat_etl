package vng.ge.stats.etl.transform.adapter


import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.utils.PathUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.sum
import vng.ge.stats.etl.transform.adapter.base.Formatter
import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.read
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
/**
 * Created by canhtq on 26/12/2016.
 */
case class App(map: Map[String,Any]) extends Serializable
case class AF(
               attributed_touch_type:String,
               event_type:String,
               app_version:String,
               af_sub1:String,
               fb_adset_id:String,

               af_sub3:String,
               af_sub2:String,
               advertising_id:String,
               af_sub5:String

             ) extends Serializable

class Mobile extends Formatter ("mobile") {

    import sqlContext.implicits._


    def start(args: Array[String]): Unit = {
        initParameters(args)
        this->run->close()
    }

    override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
        if(logType=="mobile") {
            collectMobile(logDate, hourly)
        }
    }
    def xuserID(uid: String, gameid:String) = s"$gameid$uid"


    def collectMobile(logDate: String, hourly: String): Unit = {

        val spark = SparkEnv.getSparkSession


        val makexUserId = udf(xuserID(_:String,_:String))

        //val dailyLogin = spark.read.json(s"/ge/gamelogs/sdk/$logDate/*_Login_InfoLog-$logDate.gz").select("device_os","device_id","os","updatetime","device","userID")

        val dailyLogin = spark.read.json(s"/ge/gamelogs/sdk/$logDate/*_Login_InfoLog-$logDate.gz").select("device_os","device_id","os","updatetime","device","userID","gameID").withColumn("xuser_id",makexUserId($"userID",$"gameId"))
        dailyLogin.createOrReplaceTempView("dailyLogin")
        val dailyULogin = spark.sql("select gameID,device_os,device_id,os,device,userID,substring(updatetime,0,7) as month from dailyLogin").sort("userID").dropDuplicates("userID")
        dailyULogin.createOrReplaceTempView("ulogin")

        val pay = spark.read.json("/ge/gamelogs/sdk/2017-01-*/Log_*_DBGAdd-2017-01-*.gz").select("userID","pmcNetChargeAmt")

        pay.createOrReplaceTempView("pay")
        val upay = spark.sql("select sum(pmcNetChargeAmt) as net_amt, userID from pay group by userID")
        upay.createOrReplaceTempView("upay")
        val ds = spark.sql("select ulogin.*, upay.net_amt from ulogin LEFT OUTER JOIN upay on ulogin.userID=upay.userID")

        ds.coalesce(1).write.mode("overwrite").format("parquet").save("mobile/sdk/2017-02-16")




        implicit val formats = Serialization.formats(NoTypeHints)

        val af = spark.read.option("delimiter","\t").csv("/ge/gamelogs/appsflyer/20170227/appsflyer.20170227.log.gz").select("_c2")
        //val jaf = af.map(row=>{ read(row.getString(0))})

        val afPath="/ge/fairy/warehouse/appsflyer/install/2016-07-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").select("appsflyer_device_id","media_source").distinct().sort("appsflyer_device_id").dropDuplicates("appsflyer_device_id")

        val sdkLoginPath = "/ge/gamelogs/sdk/2017-02-*/CACK_Login_InfoLog-2017-02*.gz"
        val sdkLoginDs = spark.read.json(sdkLoginPath).select("appFlyer_id","imei","userID","advertising_id","package_name").sort("userID").dropDuplicates("userID")

        val sdkDBGPath = "/ge/gamelogs/sdk/2017-02-*/Log_CACK_DBGAdd-2017-02-*.gz"
        val sdkDBGDs = spark.read.json(sdkDBGPath).select("userID","pmcNetChargeAmt").groupBy("userID").agg(sum("pmcNetChargeAmt").alias("pmcNetChargeAmt"))
        sdkDBGDs.createOrReplaceTempView("sdkDBGDs")
        sdkLoginDs.createOrReplaceTempView("sdkLoginDs")
        val sdkDs=spark.sql("select sdkLoginDs.userID,sdkLoginDs.appFlyer_id,sdkDBGDs.pmcNetChargeAmt from sdkLoginDs LEFT OUTER JOIN sdkDBGDs on sdkLoginDs.userID=sdkDBGDs.userID")
        sdkDs.createOrReplaceTempView("sdkDs")
        afDs.createOrReplaceTempView("afDs")
        val mDs=spark.sql("select sdkDs.userID,sdkDs.appFlyer_id,sdkDs.pmcNetChargeAmt,afDs.media_source from sdkDs LEFT OUTER JOIN afDs on sdkDs.appFlyer_id=afDs.appsflyer_device_id")
        mDs.select("*").where("media_source is not null and pmcNetChargeAmt >0").show(false)
        mDs.coalesce(1).write.mode("overwrite").format("csv").save("tmp/appflyer.csv")
    }

    def appsFlyer(logDate: String, hourly: String): Unit = {
        val spark = SparkEnv.getSparkSession

        val totalAccPath="/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2017-03-01"
        val totalDs = spark.read.parquet(totalAccPath)
        totalDs.show(false)

        val dailyPay="/ge/warehouse/cack/ub/sdk_data/payment_2/2017-01-01"
        val dailyPayDs = spark.read.parquet(dailyPay)
        dailyPayDs.show(false)



        val afPath="/ge/fairy/warehouse/appsflyer/install2/*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").select("appsflyer_device_id","media_source","campaign").distinct().sort("appsflyer_device_id").dropDuplicates("appsflyer_device_id")

        val sdkLoginPath16 = "/ge/gamelogs/sdk/2016-{07,08,09,10,11,12}*/CACK_Login_InfoLog-2016-{07,08,09,10,11,12}*.gz"
        val sdkLoginPath17 = "/ge/gamelogs/sdk/2017-*/CACK_Login_InfoLog-2017*.gz"
        val sdkLoginDs6 = spark.read.json(sdkLoginPath16).select("appFlyer_id","imei","userID","advertising_id","package_name")
        val sdkLoginDs7 = spark.read.json(sdkLoginPath17).select("appFlyer_id","imei","userID","advertising_id","package_name")

        val sdkLoginDs = sdkLoginDs7.union(sdkLoginDs6).sort("userID").dropDuplicates("userID")

        val sdkDBGPath = "/ge/gamelogs/sdk/2017-02-*/Log_CACK_DBGAdd-2017-02-*.gz"
        val sdkDBGDs = spark.read.json(sdkDBGPath).select("userID","pmcNetChargeAmt").groupBy("userID").agg(sum("pmcNetChargeAmt").alias("RevVND"))
        sdkDBGDs.createOrReplaceTempView("sdkDBGDs")
        sdkLoginDs.createOrReplaceTempView("sdkLoginDs")
        val sdkDs=spark.sql("select sdkLoginDs.userID,sdkLoginDs.appFlyer_id,sdkDBGDs.RevVND from sdkLoginDs LEFT OUTER JOIN sdkDBGDs on sdkLoginDs.userID=sdkDBGDs.userID")
        sdkDs.createOrReplaceTempView("sdkDs")
        afDs.createOrReplaceTempView("afDs")
        val mDs=spark.sql("select sdkDs.userID,sdkDs.appFlyer_id,sdkDs.RevVND,afDs.media_source,afDs.campaign from sdkDs LEFT OUTER JOIN afDs on sdkDs.appFlyer_id=afDs.appsflyer_device_id")
        mDs.createOrReplaceTempView("mDs")
        totalDs.createOrReplaceTempView("totalDs")

        val finalDs=spark.sql("select mDs.userID,mDs.appFlyer_id,mDs.RevVND,mDs.media_source,mDs.campaign,totalDs.log_date as register_date from mDs LEFT OUTER JOIN totalDs on mDs.userID=totalDs.id")
        finalDs.show(false)

    }

    def getData(logDate: String, hourly: String): DataFrame = {
        var afRaw: RDD[String] = null
        val lDate: String = logDate.replace("-", "")
        val patternAf = Constants.GAMELOG_DIR + s"/appsflyer/$lDate/*"
        val path = PathUtils.generateLogPathDaily(patternAf, logDate)
        afRaw = getRawLog(path)
        val trimQuote = (s: String) => {
            s.replace("\"", "")
        }
        val getValue = (line: Array[String], columnName: String) => {
            var re = ""
            for (x <- line) {
                if (x.split(":").length == 2) {
                    if (trimQuote(x.split(":")(0)).contains(columnName)) {
                        re = trimQuote(x.split(":")(1))
                    }
                }
            }
            re
        }
        val rddAf2 = afRaw.map(line => line.split("\t")).map { line =>
            val abc = line(2)
            (abc)
        }
        val afDs = rddAf2.map(line => line.split(",")).map { line =>
            val appId = getValue(line, "app_id")
            val appDeviceId = getValue(line, "appsflyer_device_id")
            val name = getValue(line, "bundle_id")
            val datetime = logDate
            val media = getValue(line, "media_source")
            val campaign = getValue(line, "campaign")
            val fb_adgroup_id = getValue(line, "fb_adgroup_id")
            val af_channel = getValue(line, "af_channel")
            val imei = getValue(line, "imei")
            val platform = getValue(line, "platform")
            (appId, appDeviceId, name, datetime, media, campaign, fb_adgroup_id, af_channel, imei, platform)
        }.toDF("app_id", "appsflyer_device_id", "bundle_id", "DATETIME"
            , "media_source", "campaign", "fb_adgroup_id"
            , "af_channel", "imei", "platform")
        afDs
    }
    def mapping(logDate: String, hourly: String): Unit = {
        val fmedia  = (media:String)=> {
            var result = "Others";
            if (media != null) {
                result = media.trim.replace(" ", "").toLowerCase

            }
            result
        }
        val mediaf = udf(fmedia)
        val spark = SparkEnv.getSparkSession
        val afPath="/ge/fairy/warehouse/appsflyer/install3/2016-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").select("install_time","appsflyer_device_id","media_source","platform","campaign").withColumn("date",substring(col("install_time"),0,10))

        afDs.createOrReplaceTempView("afDs")
        val mapSrcPath="tmp/map-src-appflyer.csv"
        val srcDs = spark.read.option("delimiter",",").option("header","true").csv(mapSrcPath).withColumn("src",mediaf(col("media_source"))).sort("src").dropDuplicates("src")
        srcDs.createOrReplaceTempView("srcDs")
        val mapDs = spark.sql("select afDs.*,srcDs.source as source from afDs LEFT OUTER JOIN srcDs on afDs.src=srcDs.src")
        mapDs.createOrReplaceTempView("mapDs")
        val rs = spark.sql("select date,source,platform,count(distinct(appsflyer_device_id)) as installs from mapDs group by date,source,platform order by date")
        rs.coalesce(1).write.mode("overwrite").format("csv").save("tmp/appflyer.csv")


        val rPath ="/ge/warehouse/cack/ub/sdk_data/flyer_result/2016-07-*"
        val ds = spark.read.option("delimiter",",").csv(rPath).toDF("date","media_src","platform","installs","nru0","rev7").withColumn("src",mediaf(col("media_src")))
        ds.createOrReplaceTempView("rev7Ds")
        val mapDs7 = spark.sql("select rev7Ds.*,srcDs.source as source from rev7Ds LEFT OUTER JOIN srcDs on rev7Ds.src=srcDs.src")
        mapDs7.createOrReplaceTempView("mapDs7")
        val rs7 = spark.sql("select date,source,platform,count(installs) as install, cast(sum(rev7) as Long) as rev7 from mapDs7 group by date,source,platform order by date")
        rs7.coalesce(1).write.mode("overwrite").format("csv").save("tmp/rs7.csv")
    }
    def jxm(logDate: String, hourly: String): Unit = {
        val spark = SparkEnv.getSparkSession
        val month="2017-03*"
        val totalMonth="2017-03-15"
        val allUserPath=s"/ge/warehouse/jxm/ub/data/activity_2/$totalMonth"
        val allDs= spark.read.parquet(allUserPath)
        val payPath=s"/ge/warehouse/jxm/ub/data/payment_2/$month"
        val payDs= spark.read.parquet(payPath)
        allDs.createOrReplaceTempView("allDs")
        payDs.createOrReplaceTempView("payDs")

        val mapDs = spark.sql("select payDs.*, allDs.log_date as reg_time from payDs LEFT OUTER JOIN allDs on payDs.id=allDs.id").withColumn("date",substring(col("log_date"),0,10)).withColumn("reg_date",substring(col("reg_time"),0,10))
        val rs = mapDs.groupBy("id","reg_date","date").sum("net_amt")
        rs.coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$month-jxm-daily-rev.csv")

        val mtext="2017-03-*"
        val actPath=s"/ge/warehouse/jxm/ub/data/activity_2/$mtext"
        val actDs= spark.read.parquet(actPath).withColumn("date",substring(col("log_date"),0,10)).select("date","id","did","os")
        actDs.createOrReplaceTempView("actDs")
        val rrs = spark.sql("select date,os,count(distinct(id)) as active, count(distinct(did)) as did from actDs group by date, os")
        rrs.coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$mtext-jxm-active.csv")
    }
    def map2(logDate: String, hourly: String): Unit = {
        val spark = SparkEnv.getSparkSession
        var pathApp = "/ge/warehouse/cack/ub/sdk_data/appsflyer/solu3/*"

        val install = spark.read.parquet(pathApp).withColumn("date", substring(col("install_time"), 0, 10)).where("appFlyer_id!=null").where("date='2016-07-07'")

        val sdkGameAddPath = "/ge/gamelogs/sdk/2016-07-{02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime", "userID", "Cost")
        install.createOrReplaceTempView("install")
        pay.createOrReplaceTempView("pay")
        val rs = spark.sql("select pay.*,install.media_source,install.platform, install.appFlyer_id from pay, install where pay.userID = install.userID ")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)




    }
    def map3(logDate: String, hourly: String): Unit = {
        val spark = SparkEnv.getSparkSession
        val afPath="/ge/fairy/warehouse/appsflyer/install3/2016-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").withColumn("date",substring(col("install_time"),0,10)).where("date='2016-07-01'")
        afDs.createOrReplaceTempView("afDs")
        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id")
        login.createOrReplaceTempView("login")
        val install = spark.sql("select login.appFlyer_id,login.userID,afDs.media_source,afDs.platform from login, afDs where login.appFlyer_id = afDs.appsflyer_device_id")
        install.createOrReplaceTempView("install")

        val totalPath ="/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2016-06-30"
        val total = spark.read.parquet(totalPath)
        total.createOrReplaceTempView("total")
        val newDs = spark.sql("select * from install LEFT OUTER JOIN total on total.id=install.userID").where("id is null")
        newDs.createOrReplaceTempView("newDs")
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-02','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,newDs.media_source,newDs.platform, newDs.appFlyer_id from pay, newDs where pay.userID = newDs.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)

    }


    def map4(logDate: String, hourly: String): Unit = {
        val spark = SparkEnv.getSparkSession


        val afPath="/ge/fairy/warehouse/appsflyer/install3/2016-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").withColumn("date",substring(col("install_time"),0,10)).where("date='2016-07-01'").withColumn("uidfa",upper(col("idfa"))).withColumn("uandroid_id",upper(col("android_id"))).withColumn("lplatform",lower(col("platform")))
        afDs.createOrReplaceTempView("afDs")
        afDs.cache()
        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
        login.createOrReplaceTempView("login")
        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.lplatform as platform from afDs, login where login.udevice_id=afDs.uidfa and afDs.lplatform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.lplatform  as platform from afDs, login where login.uandroid_id=afDs.uandroid_id and afDs.lplatform='android'")

        val install = installIOS.union(installAND)
        install.cache()
        install.createOrReplaceTempView("install")

        val totalPath ="/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2016-06-30"
        val total = spark.read.parquet(totalPath)
        total.createOrReplaceTempView("total")
        val newDs = spark.sql("select * from install LEFT OUTER JOIN total on total.id=install.userID").where("id is null")
        newDs.createOrReplaceTempView("newDs")
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-02','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,newDs.media_source,newDs.platform, newDs.appFlyer_id from pay, newDs where pay.userID = newDs.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)


        spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install, count(distinct(userID)) as nru from install group by media_source, platform").show
        afDs.select("appsflyer_device_id").distinct().count()
        install.select("appsflyer_device_id").distinct().count()
        installIOS.select("appsflyer_device_id").distinct().count()
        installAND.select("appsflyer_device_id").distinct().count()
        afDs.select("android_id").distinct().count()
        afDs.select("idfa").distinct().count()
    }


    def map5(logDate: String, hourly: String): Unit = {
        val spark = SparkEnv.getSparkSession


        val afPath="/ge/fairy/warehouse/appsflyer/install3/2016-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").withColumn("date",substring(col("install_time"),0,10)).where("date='2016-07-01'").withColumn("uidfa",upper(col("idfa"))).withColumn("uandroid_id",upper(col("android_id"))).withColumn("lplatform",lower(col("platform")))
        afDs.createOrReplaceTempView("afDs")
        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
        login.createOrReplaceTempView("login")
        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.lplatform as platform from afDs, login where login.udevice_id=afDs.uidfa and afDs.lplatform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.lplatform  as platform from afDs, login where login.uandroid_id=afDs.uandroid_id and afDs.lplatform='android'")

        val install = installIOS.union(installAND)

        install.createOrReplaceTempView("install")

        val totalPath ="/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2016-06-30"
        val total = spark.read.parquet(totalPath)
        total.createOrReplaceTempView("total")
        val newDs = spark.sql("select * from install LEFT OUTER JOIN total on total.id=install.userID").where("id is null")
        newDs.createOrReplaceTempView("newDs")
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-02','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,newDs.media_source,newDs.platform, newDs.appFlyer_id from pay, newDs where pay.userID = newDs.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)


        spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install, count(distinct(userID)) as nru from install group by media_source, platform").show


    }
    val getValue = (items:Array[String], key:String, upcase: Boolean)=>{
        var result="";
        items.map(kv=>{
            val kvs:Array[String] = kv.toString.split(":")
            if(kvs.apply(0).contains(key)){
                result=kvs.apply(1).replace("\"","")
            }
        })
        if(result.equals("null")){
            result =""
        }
        result = result.trim
        if(upcase){
            result.toUpperCase()
        }else{
            result
        }

    }
    def rawlogApps(logDate: String, hourly: String): Unit = {

        val spark = SparkEnv.getSparkSession
        val rpath="/ge/gamelogs/appsflyer/2016070*/appsflyer.*.log.gz"
        val raw = spark.read.option("delimiter","\t").csv(rpath)

        val ds = raw.map(row=>{
            val items:Array[String] = row.getAs[String]("_c2").replace("{","").replace("}","").split(",")
            var event_type:String=""
            var android_id:String =""
            var idfa:String =""
            var app_id:String =""
            var appsflyer_device_id:String =""
            var media_source:String =""
            var platform:String =""
            var advertising_id:String =""
            val log_date = row.getAs[String]("_c0")
            appsflyer_device_id = getValue(items,"appsflyer_device_id",true)

            android_id = getValue(items,"android_id",true)
            advertising_id = getValue(items,"advertising_id",true)
            media_source = getValue(items,"media_source",false)
            idfa = getValue(items,"idfa",true)
            platform = getValue(items,"platform",false)
            event_type = getValue(items,"event_type",false)
            app_id = getValue(items,"app_id",false)
            (log_date,event_type,app_id,appsflyer_device_id,media_source,platform,android_id,idfa,advertising_id)
        }).toDF("log_date","event_type","app_id","appsflyer_device_id","media_source","platform","android_id","idfa","advertising_id").withColumn("date",substring(col("log_date"),0,10))



        //result=23758
        ds.where("app_id in ('com.vng.cuuam','id1089824208') and event_type='install'  and date='2016-07-01'").createOrReplaceTempView("afDs")

        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("uadvertising_id",upper(col("advertising_id"))).withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
        login.createOrReplaceTempView("login")
        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform as platform from afDs, login where login.udevice_id=afDs.idfa and afDs.platform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform  as platform from afDs, login where login.uadvertising_id=afDs.advertising_id and afDs.platform='android'")

        val install = installIOS.union(installAND)

        install.createOrReplaceTempView("install")

        val totalPath ="/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2016-06-30"
        val total = spark.read.parquet(totalPath)
        total.createOrReplaceTempView("total")
        val newDs = spark.sql("select * from install LEFT OUTER JOIN total on total.id=install.userID").where("id is null")
        newDs.createOrReplaceTempView("newDs")
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-01','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,newDs.media_source,newDs.platform, newDs.appsflyer_device_id from pay, newDs where pay.userID = newDs.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)
    }


    def rawlogApps2(logDate: String, hourly: String): Unit = {

        val spark = SparkEnv.getSparkSession
        val rpath="/ge/gamelogs/appsflyer/2016070*/appsflyer.*.log.gz"
        val raw = spark.read.option("delimiter","\t").csv(rpath)

        val ds = raw.map(row=>{
            val items:Array[String] = row.getAs[String]("_c2").replace("{","").replace("}","").split(",")
            var event_type:String=""
            var android_id:String =""
            var idfa:String =""
            var app_id:String =""
            var appsflyer_device_id:String =""
            var media_source:String =""
            var platform:String =""
            var advertising_id:String =""
            val log_date = row.getAs[String]("_c0")
            appsflyer_device_id = getValue(items,"appsflyer_device_id",true)

            android_id = getValue(items,"android_id",true)
            advertising_id = getValue(items,"advertising_id",true)
            media_source = getValue(items,"media_source",false)
            idfa = getValue(items,"idfa",true)
            platform = getValue(items,"platform",false)
            event_type = getValue(items,"event_type",false)
            app_id = getValue(items,"app_id",false)
            (log_date,event_type,app_id,appsflyer_device_id,media_source,platform,android_id,idfa,advertising_id)
        }).toDF("log_date","event_type","app_id","appsflyer_device_id","media_source","platform","android_id","idfa","advertising_id").withColumn("date",substring(col("log_date"),0,10))



        //result=23758
        val afDs= ds.where("app_id in ('com.vng.cuuam','id1089824208') and event_type='install'  and date='2016-07-01'")
        afDs.createOrReplaceTempView("afDs")
        afDs.coalesce(1).write.mode("overwrite").format("parquet").save(s"tmp/appsflyer/2016-07-01")

        spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from afDs group by media_source, platform").show(false)
        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("uadvertising_id",upper(col("advertising_id"))).withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
        login.createOrReplaceTempView("login")
        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs LEFT OUTER JOIN  login on login.udevice_id=afDs.idfa where afDs.platform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs LEFT OUTER JOIN login on afDs.advertising_id = login.uadvertising_id where afDs.platform='android'")
        val installAND2 = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs LEFT OUTER JOIN login on afDs.android_id=login.uandroid_id where afDs.platform='android'")
        val ands = installAND.union(installAND2).sort("userID").dropDuplicates("userID")

        val install = installIOS.union(installAND)

        install.createOrReplaceTempView("install")

        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-01','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,install.media_source,install.platform, install.appsflyer_device_id from pay, install where pay.userID = install.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)
    }



    def rawlogApps3(logDate: String, hourly: String): Unit = {

        val spark = SparkEnv.getSparkSession

        val afDs =spark.read.parquet(s"tmp/appflyer/2016-07-01.csv")
        afDs.createOrReplaceTempView("afDs")
        spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from afDs group by media_source, platform").show(false)
        spark.sql("select platform, count(distinct(appsflyer_device_id)) as install from afDs group by platform").show(false)
        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("uadvertising_id",upper(col("advertising_id"))).withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")
        login.createOrReplaceTempView("login")


        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN  login on login.udevice_id=afDs.idfa where afDs.platform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.advertising_id = login.uadvertising_id where afDs.platform='android'")
        val installAND2 = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.android_id=login.uandroid_id where afDs.platform='android'")
        val ands = installAND.union(installAND2)

        val exds = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.appsflyer_device_id=login.appFlyer_id")
        exds.createOrReplaceTempView("exds")
        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from exds group by media_source, platform").show(false)
        val install = installIOS.union(ands).union(exds).sort("userID").dropDuplicates("userID")

        install.createOrReplaceTempView("install")
        spark.sql("select media_source, platform, count(distinct(userID)) as nru0 from install group by media_source, platform").show(false)
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-01','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,install.media_source,install.platform, install.appsflyer_device_id from pay, install where pay.userID = install.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)
    }
    def install4(logDate: String, hourly: String): Unit = {

        val spark = SparkEnv.getSparkSession

        val afPath="/ge/fairy/warehouse/appsflyer/install4/2016-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").withColumn("date",substring(col("install_time"),0,10)).where("date <='2016-07-01'")
        afDs.createOrReplaceTempView("afDs")


        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from afDs group by media_source, platform").show(false)
        //spark.sql("select platform, count(distinct(appsflyer_device_id)) as install from afDs group by platform").show(false)
        val sdkLoginPath="/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("uadvertising_id",upper(col("advertising_id"))).withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")

        login.createOrReplaceTempView("login")



        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN  login on login.udevice_id=afDs.idfa where afDs.platform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.advertising_id = login.uadvertising_id where afDs.platform='android'")
        val installAND2 = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.android_id=login.uandroid_id where afDs.platform='android'")
        val ands = installAND.union(installAND2)

        val exds = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.appsflyer_device_id=login.appFlyer_id")
        exds.createOrReplaceTempView("exds")
        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from exds group by media_source, platform").show(false)
        val install = installIOS.union(ands).union(exds).sort("userID").dropDuplicates("userID")
        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from exds group by media_source, platform").show(false)


        install.createOrReplaceTempView("install")
        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from install group by media_source, platform").show(false)
        val sdkNewPath="/ge/warehouse/cack/ub/sdk_data/accregister_2/2016-07-01"
        val regDs = spark.read.parquet(sdkNewPath)
       // val totalPath ="/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2016-06-30"
        //val total = spark.read.parquet(totalPath)
        regDs.createOrReplaceTempView("regDs")
        val newDs = spark.sql("select install.* from install JOIN regDs on install.userID=regDs.id")
        newDs.createOrReplaceTempView("newDs")
        //spark.sql("select media_source, platform, count(distinct(userID)) as nru0 from newDs group by media_source, platform").show(false)
        //spark.sql("select media_source, platform, count(distinct(userID)) as nru0 from install group by media_source, platform").show(false)
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-07-{01,02,03,04,05,06,07}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-07-{01,02,03,04,05,06,07}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10)).where("date in ('2016-07-01','2016-07-03','2016-07-04','2016-07-05','2016-07-06','2016-07-07')")
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,newDs.media_source,newDs.platform, newDs.appsflyer_device_id from pay, newDs where pay.userID = newDs.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)
    }

    def start(logDate: String, hourly: String): Unit = {

        val spark = SparkEnv.getSparkSession

        val afPath="/ge/fairy/warehouse/appsflyer/install4/2016-*"
        val afDs = spark.read.parquet(afPath).where("app_id in ('com.vng.cuuam','id1089824208')").withColumn("date",substring(col("install_time"),0,10)).where("date <='2016-06-28'")
        afDs.createOrReplaceTempView("afDs")


        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from afDs group by media_source, platform").show(false)
        //spark.sql("select platform, count(distinct(appsflyer_device_id)) as install from afDs group by platform").show(false)
        val sdkLoginPath="/ge/gamelogs/sdk/2016-06-28/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-06-28.gz"
        val login = spark.read.json(sdkLoginPath).select("updatetime","userID","device_id","android_id","advertising_id","appFlyer_id").withColumn("uadvertising_id",upper(col("advertising_id"))).withColumn("udevice_id",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uandroid_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")

        login.createOrReplaceTempView("login")



        val installIOS = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN  login on login.udevice_id=afDs.idfa where afDs.platform='ios'")
        val installAND = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.advertising_id = login.uadvertising_id where afDs.platform='android'")
        val installAND2 = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.android_id=login.uandroid_id where afDs.platform='android'")
        val ands = installAND.union(installAND2)

        val exds = spark.sql("select afDs.appsflyer_device_id,login.userID,afDs.media_source,afDs.platform from afDs JOIN login on afDs.appsflyer_device_id=login.appFlyer_id")
        exds.createOrReplaceTempView("exds")
        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from exds group by media_source, platform").show(false)
        val install = installIOS.union(ands).union(exds).sort("userID").dropDuplicates("userID")
        //spark.sql("select media_source, platform, count(distinct(appsflyer_device_id)) as install from exds group by media_source, platform").show(false)


        install.createOrReplaceTempView("install")

        //spark.sql("select media_source, platform, count(distinct(userID)) as nru0 from newDs group by media_source, platform").show(false)
        //spark.sql("select media_source, platform, count(distinct(userID)) as nru0 from install group by media_source, platform").show(false)
        val sdkGameAddPath="/ge/gamelogs/sdk/2016-{06-28,06-29,06-30,06-31,07-01,07-02,07-03}/Log_CACK_GameAdd/Log_CACK_GameAdd-2016-{06-28,06-29,06-30,06-31,07-01,07-02,07-03}.gz"
        val pay = spark.read.json(sdkGameAddPath).select("updatetime","userID","Cost").withColumn("date",substring(col("updatetime"),0,10))
        pay.createOrReplaceTempView("pay")
        val rs= spark.sql("select pay.*,install.media_source,install.platform, install.appsflyer_device_id from pay, install where pay.userID = install.userID")
        rs.createOrReplaceTempView("rs")
        val rss = spark.sql("select media_source, platform, cast(sum(Cost*100) as Long) as rev7 from rs group by media_source, platform")
        rss.show(false)
    }
    def xcheck1(): Unit ={
        val spark = SparkEnv.getSparkSession

        val tfa = spark.read.option("delimiter",",").option("header","true").csv("tmp/cack_nru_2016-06-28_2016-07-31.csv").withColumn("date",date_format(lit(col("install date").cast("double")/1000).cast("timestamp"),"yyyy-MM-dd")).where("date='2016-07-01'").withColumn("uid",regexp_replace(col("userid"),"u_","")).withColumn("upid",upper(col("id")))

        tfa.createOrReplaceTempView("tfa")
        spark.sql("select media_source, os, count(distinct(userid)), count(distinct(id))  from tfa group by media_source, os").show(false)
        val sdklogin = spark.read.json("/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz").select("userID","android_id","device_id","advertising_id").withColumn("did",upper(regexp_replace(col("device_id"),"_","-"))).withColumn("uan_id",upper(col("android_id"))).sort("userID").dropDuplicates("userID")

        sdklogin.createOrReplaceTempView("sdklogin")



        val install4= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4/2016-07-01")
        val install41= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4_1/2016-07-01").withColumn("android_id",upper(col("android_id"))).withColumn("advertising_id",upper(col("advertising_id")))
        val install = install4.union(install41)
        val iand = install.select("android_id").distinct()
        iand.createOrReplaceTempView("iand")
        val xand = spark.sql("select tfa.*,iand.* from tfa left outer join iand on tfa.upid=iand.android_id where tfa.os='android'")

        xand.createOrReplaceTempView("xand")
        xand.select("id").where("android_id is null").count
        val install3= spark.read.parquet("/ge/fairy/warehouse/appsflyer/install3/2016-06-28")
        install.createOrReplaceTempView("install")
        val ios = spark.sql("select install.appsflyer_device_id, sdklogin.userID from install, sdklogin where install.idfa = sdklogin.did and install.platform='ios'")
        val android1 = spark.sql("select install.appsflyer_device_id, sdklogin.userID from install, sdklogin where install.advertising_id = sdklogin.advertising_id and install.platform='android'")
        val android2 = spark.sql("select install.appsflyer_device_id, sdklogin.userID from install, sdklogin where install.android_id = sdklogin.android_id and install.platform='android'")
        val android = android1.union(android2).distinct()
        android.createOrReplaceTempView("android")
        val xch = spark.sql("select tfa.uid,tfa.id, android.* from tfa left outer join android on tfa.uid=android.userID")
        android.createOrReplaceTempView("android")

    }

    def xcheck2(): Unit = {
        import  sparkSession.implicits._
        val spark = SparkEnv.getSparkSession

        val tfa = spark.read.option("delimiter", ",").option("header", "true").csv("tmp/cack_nru_2016-06-28_2016-07-31.csv").withColumn("date", date_format(lit(col("install date").cast("double") / 1000).cast("timestamp"), "yyyy-MM-dd")).where("date='2016-07-01'").withColumn("uid", regexp_replace(col("userid"), "u_", "")).withColumn("upid", upper(col("id")))

        tfa.createOrReplaceTempView("tfa")

        val sdklogin = spark.read.json("/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz").select("userID", "android_id", "device_id", "advertising_id").withColumn("did", upper(regexp_replace(col("device_id"), "_", "-"))).withColumn("uan_id", upper(col("android_id"))).sort("userID").dropDuplicates("userID")

        sdklogin.createOrReplaceTempView("sdklogin")


        val install4 = spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4/2016-07-01")
        val install41 = spark.read.parquet("/ge/fairy/warehouse/appsflyer/install4_1/2016-07-01").withColumn("android_id", upper(col("android_id"))).withColumn("advertising_id", upper(col("advertising_id")))
        val install = install4.union(install41)
        val iand = install.select("android_id").distinct()
        iand.createOrReplaceTempView("iand")
        val xand = spark.sql("select tfa.*,iand.* from tfa left outer join iand on tfa.upid=iand.android_id where tfa.os='android'")


        val df = spark.read.json("/ge/gamelogs/sdk/2016-06-28/CACK_Login_OpenApp/CACK_Login_OpenApp-2016-06-28.gz").withColumn("did",upper(regexp_replace(col("device_id"), "_", "-")))
        val finDs = df.map(row=>{
            val referrer = row.getAs[String]("referrer")


            val app_id = row.getAs[String]("package_name")
            val android_id = row.getAs[String]("os_id")
            val device_id = row.getAs[String]("did")
            val device_os = row.getAs[String]("device_os").toLowerCase
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


            ("2017-01-18","open_app",app_id,appFlyer_id, media_source,device_os,updatetime,device_id,android_id,advertising_id,campain)
        }).toDF("logdate","event_type", "app_id", "appsflyer_device_id", "media_source", "platform",
            "install_time", "idfa", "android_id", "advertising_id","campaign")


        val sl4 =  spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/appsflyer/solu4/2016-06-28")

        val newreg =  spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/total_login_acc_2/2016-06-28")
        newreg.createOrReplaceTempView("newreg")
        sl4.createOrReplaceTempView("sl4")
        spark.sql("select count(distinct(android_id)) from sl4").show
        spark.sql("select count(distinct(newreg.id)) from sl4,newreg where sl4.userID = newreg.id").show


        val openAppDs =  spark.read.parquet("/user/zdeploy/tmp/openAppDs/2016-06-28")
        val apiCallDs =   spark.read.parquet("/user/zdeploy/tmp/apiCallDs/2016-06-28")
        val pushDs =  spark.read.parquet("/user/zdeploy/tmp/pushDs/2016-06-28")
    }

}