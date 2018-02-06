package vng.ge.stats.etl.transform.adapter

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import vng.ge.stats.etl.db.MysqlDB
import org.apache.spark.SparkContext
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, Formatter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.Common
import vng.ge.stats.report.util.{DateTimeUtils, Logger}

import scala.util.Try
import scala.util.matching.Regex


/**
  * Created by quangctn on 08/02/2017.
  */
class Jxm_Marketing extends FairyFormatter("Jxm_Marketing") {

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

  private val getOs = udf { (sid: String) => {
    var os = ""
    if (sid.startsWith("9")) {
      os = "android"
    } else if (sid.startsWith("6")) {
      os = "ios"
    }
    os
  }
  }

  def createOsNlmb(logDate: String, spark: SparkSession): Unit = {
    val sf = Constants.FIELD_NAME
    var dfActive = spark.read.parquet(s"/tmp/quangctn/nlmb/data/activity_2/$logDate")
    //var dfPayment = spark.read.parquet(s"/tmp/quangctn/nlmb/data/payment_2/$logDate")
    dfActive = dfActive.withColumn(sf.OS, getOs(col(sf.SID)))
    //dfPayment = dfPayment.withColumn(sf.OS,getOs(col(sf.SID)))
    dfActive.coalesce(1).write.mode("overwrite").format("parquet").save(s"/ge/warehouse/nlmb/ub/data/activity_2/$logDate")
    //dfPayment.coalesce(1).write.mode("overwrite").format("parquet").save(s"/ge/warehouse/nlmb/ub/data/payment_2/$logDate")

  }

  def getCcuDs(logDate: String, codeGame: String, spark: SparkSession): DataFrame = {
    import sqlContext.implicits._
    val sf = Constants.FIELD_NAME
    var ccu2: DataFrame = null
    val ccu = spark.read.json(s"/ge/gamelogs/mrtg/flume_ccu_result_$logDate.log")
      .select(explode(col("group_server"))).select("col.*")
    val ccuRaw = ccu.withColumn("server_list", explode($"server_list"))
      .select("unixtime", "server_list", "cmdb_prd_code").map { line => line(0) + "," + line(1) + "," + line(2) }.rdd
    ccu2 = ccuRaw.map(line => line.split(",")).map { r =>
      val timeStamp = MyUdf.timestampToDate((r(0)).toLong * 1000)
      val ccuStr = (r(1)).replace("[", "")
      Common.logger("ccu string: " + ccuStr)
      val sid = r(2)
      val cmdb_prd_code = r(r.length - 1)
      (cmdb_prd_code, timeStamp, ccuStr, sid)
    }.toDF("cmdb_prd_code", sf.LOG_DATE, sf.CCU, sf.SID)
    ccu2 = ccu2.selectExpr("cmdb_prd_code", sf.LOG_DATE, "cast(ccu as long)", sf.SID)
    val pathCcu = "/ge/fairy/master_data/gamecode_ccu.csv"
    Common.logger("Path file: " + pathCcu)
    val df = spark.read.option("header", true).csv(pathCcu)
    var join = ccu2.as('a).join(df.as('b), ccu2("cmdb_prd_code") === df("codeCcu"), "left_outer").where("codeCcu is not null")
    join = join.withColumnRenamed("codeGame", sf.GAME_CODE).drop("codeCcu", "cmdb_prd_code")
    join
  }

  //Generate parquet file to folder ge/fairy/warehouse
  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {

    /*writeTotalLogin_MyplayNonCard(sparkContext, sparkSession, logDate)*/
    /* writeTotalPaid_MyplayNonCard(sparkContext, sparkSession, logDate)*/
    /* createOsNlmb(logDate, sparkSession)*/

  }

  def testReadDb(spark: SparkSession, gameCode: String): DataFrame = {
    /*val query = """(select * from game_kpi where game_code='gnm' and report_date between '2017-01-01' and '2017-11-01' and kpi_id in (28060,28090,28120,28150,28180) ) abc """
    val df = spark.read.format("jdbc").
      option("url", "jdbc:mysql://10.60.22.2/ubstats").
      option("driver", "com.mysql.jdbc.Driver").
      option("user", "ubstats").
      option("password", "pubstats").
      option("dbtable",query).
      load()
    df.withColumn("rr",regexp_replace(col("kpi_id"),"28","rr")).select("report_date","game_code","rr","kpi_value").orderBy(desc("report_date")).coalesce(1).write.mode("overwrite").option("header",true).format("csv").save("/tmp/quangctn/rr/gnm/gnm.csv")*/
    emptyDataFrame
  }

  def writeTotalLogin_MyplayNonCard(sc: SparkContext, spark: SparkSession, logDate: String): Unit = {

    import spark.implicits._
    import vng.ge.stats.etl.constant.Constants
    import vng.ge.stats.report.util.DateTimeUtils


    val sf = Constants.FIELD_NAME
    val hydraDir = "/ge/hydra/warehouse/"
    val path = s"$hydraDir{ctpgsn,myplay_bida,myplay_bidamobile,myplay_bidacard,myplay_ccn,myplay_cotuong,myplay_coup,myplay_thoiloan,myplay_caro}" +
      s"/rp_myplay_a1/$logDate/*.gz"
    val ds = sc.textFile(path)
    var totalLogin = ds.map(line => line.split("\t")).map { line =>
      val id = line(0)
      ("myplay_noncard", logDate, id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    totalLogin = totalLogin.dropDuplicates(sf.ID)

    if (!logDate.startsWith("2016-01-01")) {
      val prevDate = DateTimeUtils.getPreviousDate(logDate, "a1")

      val prevPath = s"/ge/fairy/warehouse/myplay_noncard/ub/data/total_login_acc_2/$prevDate"
      val prevDs = spark.read.parquet(prevPath)
      totalLogin = totalLogin.union(prevDs).dropDuplicates(sf.ID)
    }
    val output = s"/ge/fairy/warehouse/myplay_noncard/ub/data/total_login_acc_2/$logDate"
    storeInParquet(totalLogin, output)

    //final
    if (logDate.startsWith("2016-12-31")) {
      val path2 = s"$hydraDir/famery/rp_active_1/2016-*/*.gz"
      var famery = sc.textFile(path2).map(line => line.split("\t")).map { line =>
        val id = line(0)
        ("myplay_noncard", logDate, id)
      }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
      famery = famery.dropDuplicates(sf.ID)
      totalLogin = totalLogin.union(famery).dropDuplicates(sf.ID)
      val output = s"/ge/fairy/warehouse/myplay_noncard/ub/data/total_login_acc_2/$logDate"
      storeInParquet(totalLogin, output)
    }

  }

  def writeTotalPaid_MyplayNonCard(sc: SparkContext, spark: SparkSession, logDate: String): Unit = {
    import spark.implicits._
    import vng.ge.stats.etl.constant.Constants
    import vng.ge.stats.report.util.DateTimeUtils
    import vng.ge.stats.etl.utils.Common
    var arrPath: Array[String] = Array()
    val sf = Constants.FIELD_NAME
    val hydraDir = "/ge/hydra/warehouse/"
    val path = s"$hydraDir{ctpgsn,myplay_bida,myplay_bidamobile,myplay_bidacard,myplay_ccn,myplay_cotuong,myplay_coup,myplay_thoiloan,myplay_caro,famery}" +
      s"/rp_revenue_1/$logDate/*.gz"
    Common.logger("path_=" + path)
    arrPath = Array(path)
    val ds = sc.textFile(arrPath.mkString(","))
    var totalPayment = ds.map(line => line.split("\t")).map { line =>
      val id = line(0)
      ("myplay_noncard", logDate, id)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    totalPayment = totalPayment.dropDuplicates(sf.ID)
    if (!logDate.startsWith("2016-01-01")) {
      val prevDate = DateTimeUtils.getPreviousDate(logDate, "a1")
      val prevPath = s"/ge/fairy/warehouse/myplay_noncard/ub/data/total_paid_acc_2/$prevDate"
      val prevDs = spark.read.parquet(prevPath)
      totalPayment = totalPayment.union(prevDs).dropDuplicates(sf.ID)
    }
    val output = s"/ge/fairy/warehouse/myplay_noncard/ub/data/total_paid_acc_2/$logDate"
    storeInParquet(totalPayment, output)
  }

  def getData(sc: SparkContext, spark: SparkSession) = {

    val path = "/ge/warehouse/jxm/ub/data/payment_2/*"
    var ds = spark.read.load(path)
    ds = ds.withColumn("newDate", col("log_date").substr(0, 7))
    ds = ds.orderBy(desc("newDate"))
    val andrSDs = ds.where("os like '%android%'")
    val iosSDs = ds.where("os like '%ios%'")
    val otherSDs = ds.where("os like '%other%'")

    var total = ds.groupBy("newDate").agg(countDistinct("id") as "total", sum("gross_amt") as "tRev")
      .selectExpr("newDate", "total", "cast(tRev as long)")

    total.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/total.csv")
    //1
    val androidDs = andrSDs.where("os like '%android%'").groupBy("newDate")
      .agg(countDistinct("id") as "numberAndr", sum("gross_amt") as "tRev")
      .selectExpr("newDate", "numberAndr", "cast(tRev as long)")
    androidDs.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/android.csv")
    //2
    val iosDs = iosSDs.where("os like '%ios%'").groupBy("newDate")
      .agg(countDistinct("id") as "numberIos", sum("gross_amt") as "tRev")
      .selectExpr("newDate", "numberIos", "cast(tRev as long)")
    iosDs.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/ios.csv")
    //3
    val otherDs = otherSDs.where("os like '%other%'").groupBy("newDate")
      .agg(countDistinct("id") as "numberOther", sum("gross_amt") as "tRev")
      .selectExpr("newDate", "numberOther", "cast(tRev as long)")
    otherDs.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/other.csv")
    //4

    val idAndIos = andrSDs.as('a)
      .join(iosSDs.as('b), andrSDs("id") === iosSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate").dropDuplicates("id")
    idAndIos.groupBy("newDate").agg(countDistinct("id")).coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/id_andrios.csv")

    val SumAndIos = idAndIos.as('a)
      .join(ds.as('b), idAndIos("id") === ds("id") and idAndIos("newDate") === ds("newDate"), "left_outer")
      .where("b.id is not null").groupBy("a.newDate").agg(sum("gross_amt") as "tRev")
      .selectExpr("newDate", "cast(tRev as long)")
    SumAndIos.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/android_ios_2.csv")
    //5*/

    val idAndOther = andrSDs.as('a)
      .join(otherSDs.as('b), andrSDs("id") === otherSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate").dropDuplicates("id")
    idAndOther.groupBy("newDate").agg(countDistinct("id")).coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/id_andother.csv")

    val SumAndOther = idAndOther.as('a)
      .join(ds.as('b), idAndOther("id") === ds("id") and idAndOther("newDate") === ds("newDate"), "left_outer")
      .where("b.id is not null").groupBy("a.newDate").agg(sum("gross_amt") as "tRev")
      .selectExpr("newDate", "cast(tRev as long)")
    SumAndOther.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/android_other_2.csv")

    //6
    val idOtherIos = iosSDs.as('a)
      .join(otherSDs.as('b), iosSDs("id") === otherSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate").dropDuplicates("id")
    idOtherIos.groupBy("newDate").agg(countDistinct("id")).coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/id_iosother.csv")
    val SumIosOther = idOtherIos.as('a)
      .join(ds.as('b), idOtherIos("id") === ds("id") and idOtherIos("newDate") === ds("newDate"), "left_outer")
      .where("b.id is not null").groupBy("a.newDate").agg(sum("gross_amt") as "tRev")
      .selectExpr("newDate", "cast(tRev as long)")
    SumIosOther.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/ios_other_2.csv")
    //7

    val idAll = idOtherIos.as('a)
      .join(andrSDs.as('b), idOtherIos("id") === andrSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate").dropDuplicates("id")
    idAll.groupBy("newDate").agg(countDistinct("id")).coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/id_all.csv")

    val SumAll = idAll.as('a)
      .join(ds.as('b), idAll("id") === ds("id") and idAll("newDate") === ds("newDate"), "left_outer")
      .where("b.id is not null").groupBy("a.newDate").agg(sum("gross_amt") as "tRev")
      .selectExpr("newDate", "cast(tRev as long)")
    SumAll.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/all_3.csv")
  }


  def getData2(sc: SparkContext, spark: SparkSession) = {
    import spark.implicits._
    val path = "/ge/warehouse/jxm/ub/data/payment_2/2017-{01,02}-*"
    var ds = spark.read.load(path)
    ds = ds.withColumn("newDate", col("log_date").substr(0, 7))
    ds = ds.orderBy(desc("newDate"))
    val andrSDs = ds.where("os like '%android%'")
    val iosSDs = ds.where("os like '%ios%'")
    val otherSDs = ds.where("os like '%other%'")
    //Paying User Data
    //1.Total
    var total = ds.groupBy("newDate").agg(countDistinct("id") as "total")
      .selectExpr("newDate", "total")
    //2.Android
    val androidDs = andrSDs.groupBy("newDate")
      .agg(countDistinct("id") as "android")
      .selectExpr("newDate", "android")
    //3.Ios
    val iosDs = iosSDs.groupBy("newDate")
      .agg(countDistinct("id") as "ios")
      .selectExpr("newDate", "ios")
    //Other
    val otherDs = otherSDs.groupBy("newDate")
      .agg(countDistinct("id") as "other")
      .selectExpr("newDate", "other")

    total = total.as('a)
      .join(androidDs.as('b), Seq("newDate"), "left_outer")
      .join(iosDs, Seq("newDate"), "left_outer")
      .join(otherDs, Seq("newDate"), "left_outer")

    // Android  + IOS
    val idAndIos1 = andrSDs.as('a)
      .join(iosSDs.as('b), andrSDs("id") === iosSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate", "a.gross_amt").dropDuplicates("id")
    val idAndIos2 = idAndIos1.groupBy("newDate")
      .agg(countDistinct("id") as "and_ios").withColumnRenamed("newDate", "newDate1")
      .selectExpr("newDate1", "and_ios")


    total = total.as('a).join(idAndIos2.as('b), 'newDate === 'newDate1)
    total = total.drop("newDate1")

    //Android + Other
    val idAndOther1 = andrSDs.as('a)
      .join(otherSDs.as('b), andrSDs("id") === otherSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate", "a.gross_amt").dropDuplicates("id")
    val idAndOther2 = idAndOther1.groupBy("newDate")
      .agg(countDistinct("id") as "and_other").withColumnRenamed("newDate", "newDate2")
      .selectExpr("newDate2", "and_other")

    total = total.as('a).join(idAndOther2.as('b), 'newDate === 'newDate2)
    total = total.drop("newDate2")

    //Other + IOS
    val idOtherIos1 = iosSDs.as('a)
      .join(otherSDs.as('b), iosSDs("id") === otherSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate", "a.gross_amt").dropDuplicates("id")

    val idOtherIos2 = idOtherIos1
      .groupBy("newDate")
      .agg(countDistinct("id") as "other_ios").withColumnRenamed("newDate", "newDate3")
      .selectExpr("newDate3", "other_ios")

    total = total.as('a).join(idOtherIos2.as('b), 'newDate === 'newDate3)
    total = total.drop("newDate3")


    //Android + IOS + Other

    val idAll1 = idOtherIos1.as('a)
      .join(andrSDs.as('b), idOtherIos1("id") === andrSDs("id"), "left_outer")
      .where("b.id is not null").select("a.id", "a.newDate", "a.gross_amt").dropDuplicates("id")
    val idAll2 = idAll1.groupBy("newDate")
      .agg(countDistinct("id") as "and_ios_other").withColumnRenamed("newDate", "newDate4")
      .selectExpr("newDate4", "and_ios_other")
    total = total.as('a).join(idAll2.as('b), 'newDate === 'newDate4)
    total = total.drop("newDate4")
    //total.coalesce(1).write.format("csv").save("/user/fairy/quangctn/jxm_marketing/PU.csv")

    //Total Rev
    var rev = ds.groupBy("newDate").agg(sum("gross_amt") as "total")
      .selectExpr("newDate", "cast(total as long)")
    //1.Android
    val rev_androidDs = andrSDs.groupBy("newDate")
      .agg(sum("gross_amt") as "android")
      .selectExpr("newDate", "cast(android as long)")
    //2.IOS
    val rev_iosDs = iosSDs.groupBy("newDate")
      .agg(sum("gross_amt") as "ios")
      .selectExpr("newDate", "cast(ios as long)")
    //3.Other
    val rev_otherDs = otherSDs.groupBy("newDate")
      .agg(sum("gross_amt") as "other")
      .selectExpr("newDate", "cast(other as long)")

    rev = rev.as('a)
      .join(rev_androidDs.as('b), Seq("newDate"), "left_outer")
      .join(rev_iosDs, Seq("newDate"), "left_outer")
      .join(rev_otherDs, Seq("newDate"), "left_outer")
    //4 Android + IOS
    val rev_idAndIos2 = idAndIos1.groupBy("newDate")
      .agg(sum("gross_amt") as "and_ios").withColumnRenamed("newDate", "newDate1")
      .selectExpr("newDate1", "and_ios")
    rev = rev.as('a).join(rev_idAndIos2.as('b), 'newDate === 'newDate1)
    rev = rev.drop("newDate1")
    //5 Android + Other
    val rev_idAndOther2 = idAndOther1.groupBy("newDate")
      .agg(sum("gross_amt") as "and_other").withColumnRenamed("newDate", "newDate2")
      .selectExpr("newDate2", "and_other")
    rev = rev.as('a).join(rev_idAndOther2.as('b), 'newDate === 'newDate2)
    rev = rev.drop("newDate2")
    //6 Ios + Other

    val rev_idOtherIos2 = idOtherIos1
      .groupBy("newDate")
      .agg(sum("gross_amt") as "other_ios").withColumnRenamed("newDate", "newDate3")
      .selectExpr("newDate3", "other_ios")
    rev = rev.as('a).join(rev_idOtherIos2.as('b), 'newDate === 'newDate3)
    rev = rev.drop("newDate3")
    //7 Android + Ios + Other
    val rev_idAll2 = idAll1.groupBy("newDate")
      .agg(sum("gross_amt") as "android_other_ios").withColumnRenamed("newDate", "newDate4")
      .selectExpr("newDate4", "android_other_ios")
    rev = rev.as('a).join(rev_idAll2.as('b), 'newDate === 'newDate4)
    rev = rev.drop("newDate4")


  }

  //RID + SID
  def exportFileGnm2_rid_sid(sc: SparkContext, spark: SparkSession) = {
    var csv_2 = spark.read.csv("/user/fairy/quangctn/gnm2/sheet2.csv")
    csv_2 = csv_2.withColumnRenamed("_c2", "rid")
    csv_2 = csv_2.withColumnRenamed("_c3", "sid")

    val ds_gnm = spark.read.load("/ge/warehouse/gnm/ub/data/activity_2/{2016,2017}-{12,01,02,03}-*")
      .dropDuplicates("rid", "sid")
    var sheet2 = csv_2.as('a).join(ds_gnm.as('b),
      csv_2("rid") === ds_gnm("rid") and csv_2("sid") === ds_gnm("sid"), "left_outer")
      .select("_c0", "_c1", "a.rid", "a.sid", "b.os", "_c4", "_c5")
    sheet2 = sheet2.withColumn("date", unix_timestamp(col("_c5"), "MM/dd/yyyy HH:mm:ss")).orderBy(asc("date"))
    sheet2.coalesce(1).write.format("csv").save("/user/fairy/quangctn/gnm2/result_sheet2_rid_sid.csv")
  }

  def exportFileGnm1_rid_sid(sc: SparkContext, spark: SparkSession) = {
    var csv_1 = spark.read.csv("/user/fairy/quangctn/gnm2/sheet1.csv")
    csv_1 = csv_1.withColumnRenamed("_c2", "rid")
    csv_1 = csv_1.withColumnRenamed("_c3", "sid")
    val ds_gnm = spark.read.load("/ge/warehouse/gnm/ub/data/activity_2/{2016,2017}-{12,01,02,03}-*")
      .dropDuplicates("rid", "sid")
    var sheet1 = csv_1.as('a).join(ds_gnm.as('b),
      csv_1("rid") === ds_gnm("rid") and csv_1("sid") === ds_gnm("sid"), "left_outer")
      .select("_c0", "_c1", "a.rid", "a.sid", "b.os", "_c4", "_c5")
    sheet1 = sheet1.withColumn("date", unix_timestamp(col("_c5"), "MM/dd/yyyy HH:mm:ss")).orderBy(asc("date"))
    sheet1.coalesce(1).write.format("csv").save("/user/fairy/quangctn/gnm2/sheet1_rid_sid.csv")
  }

  //TEST
  def exportFileGnm2_rid_sid1day(sc: SparkContext, spark: SparkSession) = {
    var csv_2 = spark.read.csv("/user/fairy/quangctn/gnm/sheet2.csv")
    csv_2 = csv_2.withColumnRenamed("_c2", "rid")
    csv_2 = csv_2.withColumnRenamed("_c3", "sid")

    val ds_gnm = spark.read.load("/ge/warehouse/gnm/ub/data/activity_2/2016-12-03")
      .dropDuplicates("rid", "sid")
    var sheet2 = csv_2.as('a).join(ds_gnm.as('b),
      csv_2("rid") === ds_gnm("rid") and csv_2("sid") === ds_gnm("sid"), "left_outer")
  }


  //RID
  def exportFileGnm2_rid(sc: SparkContext, spark: SparkSession) = {
    var csv_2 = spark.read.csv("/user/fairy/quangctn/gnm2/sheet2.csv")
    csv_2 = csv_2.withColumnRenamed("_c2", "rid")
    csv_2 = csv_2.withColumnRenamed("_c3", "sid")

    val ds_gnm = spark.read.load("/ge/warehouse/gnm/ub/data/activity_2/{2016,2017}-{12,01,02,03}-*")
      .dropDuplicates("rid")
    var sheet2 = csv_2.as('a).join(ds_gnm.as('b),
      csv_2("rid") === ds_gnm("rid"), "left_outer")
      .select("_c0", "_c1", "a.rid", "a.sid", "b.os", "_c4", "_c5")
    sheet2 = sheet2.withColumn("date", unix_timestamp(col("_c5"), "MM/dd/yyyy HH:mm:ss")).orderBy(asc("date"))
    sheet2.coalesce(1).write.format("csv").save("/user/fairy/quangctn/gnm2/sheet2_rid.csv")
  }

  def exportFileGnm1_rid(sc: SparkContext, spark: SparkSession) = {
    var csv_1 = spark.read.csv("/user/fairy/quangctn/gnm2/sheet1.csv")
    csv_1 = csv_1.withColumnRenamed("_c2", "rid")
    csv_1 = csv_1.withColumnRenamed("_c3", "sid")
    val ds_gnm = spark.read.load("/ge/warehouse/gnm/ub/data/activity_2/{2016,2017}-{12,01,02,03}-*")
      .dropDuplicates("rid", "sid")
    var sheet1 = csv_1.as('a).join(ds_gnm.as('b),
      csv_1("rid") === ds_gnm("rid") and csv_1("sid") === ds_gnm("sid"), "left_outer")
      .select("_c0", "_c1", "a.rid", "a.sid", "b.os", "_c4", "_c5")
    sheet1 = sheet1.withColumn("date", unix_timestamp(col("_c5"), "MM/dd/yyyy HH:mm:ss")).orderBy(asc("date"))
    sheet1.coalesce(1).write.format("csv").save("/user/fairy/quangctn/gnm2/sheet1_rid.csv")
  }

  //TEST
  def exportFileGnm2_rid1day(sc: SparkContext, spark: SparkSession) = {
    var csv_2 = spark.read.csv("/user/fairy/quangctn/gnm/sheet2.csv")
    csv_2 = csv_2.withColumnRenamed("_c2", "rid")
    csv_2 = csv_2.withColumnRenamed("_c3", "sid")
    //2016-12-03
    val ds_gnm = spark.read.load("/ge/warehouse/gnm/ub/data/activity_2/2016-12-03")
      .dropDuplicates("rid")
    var sheet2 = csv_2.as('a).join(ds_gnm.as('b),
      csv_2("rid") === ds_gnm("rid"), "left_outer")
    sheet2.coalesce(1).write.format("csv").save("/user/fairy/quangctn/gnm/sheet2.csv")
  }

  def sortCsvFile(ds: DataFrame): Unit = {
    val sheet2 = ds.withColumn("date", unix_timestamp(col("_c0"), "MM/dd/yyyy HH:mm")).orderBy(asc("date"))
  }

  // [Cửu Âm]Request log 27-04
  def exportCack(sc: SparkContext, spark: SparkSession): Unit = {
    val login = spark.read.json("/ge/gamelogs/sdk/2017-*/CACK_Login_InfoLog-2017-{03,04}*.gz")
      .select("userID", "device_id", "advertising_id", "appFlyer_id", "referrer", "android_id", "device")
      .sort("userID").dropDuplicates("userID")
    val pay = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2017-{03,04}*")
      .where("log_date >='2017-03-31' and log_date <'2017-04-26'")
      .select("log_date", "id", "gross_amt", "net_amt")

    val csv = pay.as('a).join(login.as('b), pay("id") === login("userID"), "left_outer")
      .select("log_date", "id", "gross_amt", "net_amt",
        "device_id", "advertising_id", "appFlyer_id", "referrer", "android_id", "b.device")
    csv.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save("tmp/cack/data_2.csv")

  }

  def exportDataROW(sc: SparkContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val totalLogin = spark.read.parquet("/ge/warehouse/stct/ub/sdk_data/total_login_acc_2/2017-03-31")
    val activity = spark.read.parquet("/ge/warehouse/stct/ub/sdk_data/activity_2/2017-{04,05}*")
    // List Id that no login in 30 day
    val lastLoginId = totalLogin.as('a).join(activity.as('b), totalLogin("id") === activity("id"), "left_outer")
      .where("b.id is null").select("a.log_date", "a.id")
    /* Change by
      val lastLoginId = spark.read.csv("/user/fairy/tmp/row/lastLoginId.csv")
      .withColumnRenamed("_c0","log_date")
      .withColumnRenamed("_c1","id")
     */


    val payment = spark.read.parquet("/ge/warehouse/stct/ub/sdk_data/payment_2/*")
      .withColumn("newDate", col("log_date").substr(0, 10))
      .where("newDate <'2017-03-31'").select("id", "gross_amt")
      .withColumnRenamed("id", "id_pay")
    val join1 = lastLoginId.as('a).join(payment.as('b), 'id === 'id_pay, "left_outer")
      .select("a.log_date", "a.id", "b.gross_amt")
    val re1 = join1.groupBy("id").agg(sum("gross_amt") as "total_rev", min("log_date") as "log_date")
    //re1.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save("tmp/row/re1.csv")
    /* Change by
    val re1 = spark.read.csv("/user/fairy/tmp/row/re1.csv")
    .withColumnRenamed("_c0","id")
    .withColumnRenamed("_c1","total_rev")
    .withColumnRenamed("_c2","log_date")
   */

    // join sdk log
    ///ge/gamelogs/sdk_sea/2017-01-01/STCT_Login_InfoLog/STCT_Login_InfoLog-2017-01-01.gz
    val sdk = spark.read.json("/ge/gamelogs/sdk_sea/*/STCT_Login_InfoLog-*")
      .select("userID", "device_os", "email", "device_id", "android_id", "updatetime")
      .withColumn("newDate", col("updatetime").substr(0, 10))
      .where("newDate<'2017-03-31'")
      .orderBy(desc("updatetime"))
      .dropDuplicates("userID")
    //sdk.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save("tmp/row/sdk.csv")
    /* Change by
  val sdk = spark.read.csv("/user/fairy/tmp/row/sdk.csv")
  .withColumnRenamed("_c0","userID")
  .withColumnRenamed("_c1","device_os")
  .withColumnRenamed("_c2","email")
  .withColumnRenamed("_c3","device_id")
  .withColumnRenamed("_c4","android_id")
  .withColumnRenamed("_c5","updatetime")
 */
    var re2 = re1.as('a).join(sdk.as('b), 'id === 'userID, "left_outer")
      .select("a.log_date", "a.id", "total_rev", "b.updatetime", "email", "device_id", "android_id", "device_os")
    val re3 = re2.withColumnRenamed("log_date", "first_login")
      .withColumnRenamed("updatetime", "last_login")
    re3.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save("tmp/row/export_data.csv")


  }

  def exportDataCACK(sc: SparkContext, spark: SparkSession): Unit = {
    val game_code = "cack";
    import spark.implicits._
    val totalLogin = spark.read.parquet(s"/ge/warehouse/$game_code/ub/sdk_data/total_login_acc_2/2017-03-31")
    val activity = spark.read.parquet(s"/ge/warehouse/$game_code/ub/sdk_data/activity_2/2017-{04,05}*")

    val lastLoginId = totalLogin.as('a).join(activity.as('b), totalLogin("id") === activity("id"), "left_outer")
      .where("b.id is null").select("a.log_date", "a.id")
    lastLoginId.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$game_code/lastLoginId.csv")

    val payment = spark.read.parquet(s"/ge/warehouse/$game_code/ub/sdk_data/payment_2/*")
      .withColumn("newDate", col("log_date").substr(0, 10))
      .where("newDate <='2017-03-31'").select("id", "gross_amt")
      .withColumnRenamed("id", "id_pay")
    val join1 = lastLoginId.as('a).join(payment.as('b), 'id === 'id_pay, "left_outer")
      .select("a.log_date", "a.id", "b.gross_amt")
    val re1 = join1.groupBy("id").agg(sum("gross_amt") as "total_rev", min("log_date") as "log_date")
    re1.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$game_code/re1.csv")


    val sdk = spark.read.json("/ge/gamelogs/sdk/*/CACK_Login_InfoLog-*")
      .select("userID", "device_os", "email", "device_id", "android_id", "updatetime")
      .withColumn("newDate", col("updatetime").substr(0, 10))
      .where("newDate<'2017-03-31'")
      .orderBy(desc("updatetime"))
      .dropDuplicates("userID")
    sdk.coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$game_code/sdk.csv")

    val re2 = re1.as('a).join(sdk.as('b), 'id === 'userID, "left_outer")
      .select("a.log_date", "a.id", "total_rev", "b.updatetime", "email", "device_id", "android_id", "device_os")
    val re3 = re2.withColumnRenamed("log_date", "first_login")
      .withColumnRenamed("updatetime", "last_login")
      .withColumnRenamed("id", "UserID")
      .withColumn("ZaloId", lit(""))
      .withColumn("Facebook", lit(""))
      .withColumn("Phone", lit(""))
      .withColumn("Total Playing time", lit(""))
      .withColumn("Number login", lit(""))
      .select("UserID", "ZaloId", "android_id", "device_id", "Facebook", "email", "Phone", "total_rev",
        "Total Playing time", "first_login", "Number login", "last_login")
    re3.coalesce(2).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/$game_code/export_data.csv")

  }

  def exportDataNLMB(sc: SparkContext, spark: SparkSession): Unit = {
    val game_code = "nlmb";
    import spark.implicits._
    val totalLogin = spark.read.parquet(s"/ge/warehouse/$game_code/ub/data/total_login_acc_2/2017-03-31")
    val activity = spark.read.parquet(s"/ge/warehouse/$game_code/ub/data/activity_2/2017-{04,05}*")

    val lastLoginId = totalLogin.as('a).join(activity.as('b), totalLogin("id") === activity("id"), "left_outer")
      .where("b.id is null").select("a.log_date", "a.id")
    lastLoginId.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$game_code/lastLoginId.csv")

    val payment = spark.read.parquet(s"/ge/warehouse/$game_code/ub/data/payment_2/*")
      .withColumn("newDate", col("log_date").substr(0, 10))
      .where("newDate <='2017-03-31'").select("id", "gross_amt")
      .withColumnRenamed("id", "id_pay")
    val join1 = lastLoginId.as('a).join(payment.as('b), 'id === 'id_pay, "left_outer")
      .select("a.log_date", "a.id", "b.gross_amt")
    val re1 = join1.groupBy("id").agg(sum("gross_amt") as "total_rev", min("log_date") as "log_date")
    re1.orderBy("id").coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$game_code/re1.csv")


    val sdk = spark.read.json("/ge/gamelogs/sdk/*/CACK_Login_InfoLog-*")
      .select("userID", "device_os", "email", "device_id", "android_id", "updatetime")
      .withColumn("newDate", col("updatetime").substr(0, 10))
      .where("newDate<'2017-03-31'")
      .orderBy(desc("updatetime"))
      .dropDuplicates("userID")
    sdk.coalesce(1).write.mode("overwrite").format("csv").save(s"tmp/$game_code/sdk.csv")

    val re2 = re1.as('a).join(sdk.as('b), 'id === 'userID, "left_outer")
      .select("a.log_date", "a.id", "total_rev", "b.updatetime", "email", "device_id", "android_id", "device_os")
    val re3 = re2.withColumnRenamed("log_date", "first_login")
      .withColumnRenamed("updatetime", "last_login")
      .withColumnRenamed("id", "UserID")
      .withColumn("ZaloId", lit(""))
      .withColumn("Facebook", lit(""))
      .withColumn("Phone", lit(""))
      .withColumn("Total Playing time", lit(""))
      .withColumn("Number login", lit(""))
      .select("UserID", "ZaloId", "android_id", "device_id", "Facebook", "email", "Phone", "total_rev",
        "Total Playing time", "first_login", "Number login", "last_login")
    re3.coalesce(2).write.mode("overwrite").format("csv").option("header", "true").save(s"tmp/$game_code/export_data.csv")

  }

  def etl360Live(sc: SparkContext, spark: SparkSession, logDate: String): Unit = {
    import sqlContext.implicits._
    val pathFile = "/ge/gamelogs/360live/2017-05-10/360live_payment_stats/360live_payment_stats-2017-05-10_00010"
    val paymentRaw = sc.textFile(pathFile)

    val paymentFilter = (line: String) => {
      var rs = false
      if (MyUdf.timestampToDate(line.toLong).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val getOs = (s: String) => {
      var platform = "other"
      if (s == "0") {
        platform = "ios"
      } else if (s == "1") {
        platform = "android"
      }
      platform
    }
    val convertMap = Map(
      "AUD" -> 16543,
      "VND" -> 1,
      "USD" -> 22700
    )
    val getMoney = (money: String, currency: String) => {
      var vnd = 0.0
      if (convertMap.contains(currency)) {
        vnd = money.toFloat * convertMap(currency)
      }
      vnd
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => paymentFilter(line(0))).map { line =>
      val logDate = MyUdf.timestampToDate(line(0).toLong)
      val userId = line(1)
      val os = getOs(line(2))
      val gross_amt = getMoney(line(3), line(5))
      val net_amt = getMoney(line(4), line(5))
      val currency = line(5)
      ("360live", logDate, userId, os, gross_amt, net_amt)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.OS, sf.GROSS_AMT, sf.NET_AMT)
    val activityDs = spark.read.parquet(s"/ge/fairy/warehouse/360live/ub/data/activity_2/$logDate/")
    val paymentMoreDs = getJoinInfo(paymentDs, activityDs)
    paymentMoreDs.write.mode("overwrite").parquet(s"/ge/fairy/warehouse/360live/ub/data/payment_2/$logDate/")
  }

  def dirExists(hdfsDirectory: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
    return exists
  }

  //myplay_bacay
  //myplay_xidzach
  def totalLogin_Card(sc: SparkContext, spark: SparkSession, hydraFolder: String, fairyFolder: String, logDate: String): Unit = {
    val sf = Constants.FIELD_NAME
    import spark.sqlContext.implicits._
    val output = s"/ge/fairy/warehouse/$fairyFolder/ub/data/total_login_acc_2/$logDate"
    Common.logger("output path: " + output)
    val raw = sc.textFile(s"/ge/hydra/warehouse/$hydraFolder/rp_a1_up_to_now/$logDate")
    val totalLogin = raw.map(line => line.split("\\t")).map { line =>
      val gameName = hydraFolder
      val id = line(0)
      val dateTime = line(1)
      (gameName, id, dateTime)
    }.toDF("game_code", "id", "log_date")
    totalLogin.dropDuplicates(sf.ID).coalesce(1).write.mode("overwrite")
      .parquet(output)
  }

  //
  def totalLogin_International(sc: SparkContext, spark: SparkSession, dbName: String, logDate: String): Unit = {
    val sf = Constants.FIELD_NAME
    val sql = s"select user_id as id,log_date from $dbName.user where ds <'$logDate'"
    Common.logger("sql query: " + sql)
    var totalLogin = spark.sql(sql)
    totalLogin = totalLogin.withColumn("game_code", lit(dbName))
    totalLogin = totalLogin.dropDuplicates("id")
    totalLogin.coalesce(1).write.mode("overwrite").parquet(s"/ge/fairy/warehouse/$dbName/ub/data/total_login_acc_2/$logDate")
  }

  def checkRR(spark: SparkSession, logDate: String, number: Int, gameCode: String): Unit = {
    val toDate = LocalDate.parse(logDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val prevDate = toDate.minusDays(number)
    val register2608 = spark.read.parquet(s"/ge/fairy/warehouse/$gameCode/ub/data/accregister_2/$prevDate").selectExpr("id as idReg", "os as osReg").distinct
    val activity2509 = spark.read.parquet(s"/ge/fairy/warehouse/$gameCode/ub/data/activity_2/$logDate").select("id", "os")
    var join = register2608.as('a).join(activity2509.as('b), register2608("idReg") === activity2509("id"), "left_outer").where("id is not null")
    val result = join.groupBy("os").agg(countDistinct("id")).orderBy("os")

  }

  def checkDBG(spark: SparkSession): Unit = {
    spark.read.option("delimiter", "\t").csv("/ge/gamelogs/dbg/2017-09-27/*").where("_c8 like '%Kiếm Linh%' and _c26 =='SUCCESSFUL' and _c0 like '%20170927%'").select("_c16", "_c2", "_c8", "_c1").agg(sum("_c16").cast("long")).show(false)
  }

  def checkRevMissMatch(spark: SparkSession): DataFrame = {
    val sdkRev = spark.read.json("/ge/gamelogs/sdk/2017-09-27/Log_KVM_DBGAdd/Log_KVM_DBGAdd-2017-09-27.gz").select("updatetime", "pmcNetChargeAmt", "resultCode", "roleID")
    val inRev = spark.read.option("delimiter", "\t").option("header", true).csv("/ge/gamelogs/kvm/2017-09-28/datalog/LOG_*/recharge.txt").withColumn("log_date", date_format(col("pay_time").cast("double").cast("timestamp"), "yyyy-MM-dd HH:mm:ss")).select("user_id", "srv_id", "pay_time", "is_succ", "money", "channel_id", "log_date")
    val join = inRev.as('a).join(sdkRev.as('b), inRev("user_id") === sdkRev("roleID"), "left_outer")
    join
  }

  def exportRevRangeDateGnm(startDate: String, endDate: String, pathSource: String, outPutFolder: String, spark: SparkSession): Unit = {
    var path: Array[String] = Array()
    val pattern = "/ge/warehouse/gnm/ub/data/payment_2/"
    val rangeDate = between(startDate, endDate)
    rangeDate.foreach(date => {
      path = path ++ Array(pattern + date)
    })
    val sf = Constants.FIELD_NAME
    val inGame = spark.read.parquet(path: _*).select(sf.LOG_DATE, sf.RID, sf.NET_AMT)
    val out = spark.read.option("header", true).csv(pathSource)
    var join = out.as('a).join(inGame.as('b), out("ROLE_ID") === inGame("rid"), "left_outer")
    join = join.groupBy("ROLE_ID").agg(sum(sf.NET_AMT).cast("long") as ('Sum))
    join = join.withColumn("Sum", makeOthersIfNull(col("Sum"))).withColumn(sf.GAME_CODE, lit("gnm"))
    join.coalesce(1).write.mode("overwrite").option("header", true).format("csv").save(s"/tmp/quangctn/export_gnm/$outPutFolder/gnm.csv")
  }

  val makeOthersIfNull = udf { (str: String) => {

    if (str == null || str == "") "0" else str
  }
  }
  def exportRevRangeDateGnm_xls(startDate: String, endDate: String, pathSource: String, outPutFolder: String, spark: SparkSession): Unit = {
    var path: Array[String] = Array()
    val pattern = "/ge/warehouse/gnm/ub/data/payment_2/"
    val rangeDate = between(startDate, endDate)
    rangeDate.foreach(date => {
      path = path ++ Array(pattern + date)
    })
    val sf = Constants.FIELD_NAME
    val inGame = spark.read.parquet(path: _*).select(sf.LOG_DATE, sf.RID, sf.NET_AMT)
    val out = spark.read.format("com.crealytics.spark.excel").option("useHeader", true)
      .load(pathSource)
    var join = out.as('a).join(inGame.as('b), out("ROLE_ID") === inGame("rid"), "left_outer")
    join = join.groupBy("ROLE_ID").agg(sum(sf.NET_AMT).cast("long") as ('Sum))
    join = join.withColumn("Sum", makeOthersIfNull(col("Sum"))).withColumn(sf.GAME_CODE, lit("gnm"))
    join.coalesce(1).write.mode("overwrite").option("header", true).format("csv").save(s"/tmp/quangctn/export_gnm/$outPutFolder/gnm.csv")
  }

  ///tmp/quangctn/gnm/gnm_1011
  def between(fromDate: String, toDate: String) = {
    val _toDate = LocalDate.parse(toDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val _fromDate = LocalDate.parse(fromDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    _fromDate.toEpochDay.until(_toDate.toEpochDay).map(LocalDate.ofEpochDay)
  }

  def filterTid_Muw(startDate: String, endDate: String, spark: SparkSession): Unit = {
    val rangeDate = between(startDate, endDate)
    rangeDate.foreach(date => {
      val path = s"/tmp/quangctn/muw/payment_2/$date"
      val muDf = spark.read.parquet(path).where("trans_id != '9999999999'")
      muDf.coalesce(1).write.mode("overwrite").format("parquet").save(s"/ge/fairy/warehouse/muw/ub/data/payment_2/$date")
    })
  }

  def getCcu(): Unit = {
    //log_dir=/ge/warehouse
  }

  def fixPhuclong(startDate: String, endDate: String, spark: SparkSession): Unit = {
    val sf = Constants.FIELD_NAME
    /*  val startDate = "2017-11-01"
      val endDate = "2017-11-30"*/
    val rangeDate = between(startDate, endDate)
    rangeDate.foreach(date => {
      val path = s"/tmp/quangctn/phuclong/payment/$date"
      val phuclongDf = spark.read.parquet(path).dropDuplicates(sf.TRANS_ID)
      phuclongDf.coalesce(1).write.mode("overwrite").format("parquet").save(s"/ge/fairy/warehouse/phuclong/ub/data/payment_2/$date")
    })
  }

  private val getBrand = udf { (deviceInfo: String, device_os: String) => {
    //    Common.logger("device-info: "+deviceInfo)
    var brand = ""
    if (device_os.toLowerCase.equalsIgnoreCase("ios")) {
      brand = "Apple"
    }
    if (deviceInfo != "") {
      val pattern = new Regex("((Build.BRAND.*?)_)")
      val result = (pattern findAllIn deviceInfo).mkString("")
      if (!result.equalsIgnoreCase("")) {
        //        Common.logger("vao day")
        val _brand = result.split(":")(1)
        //        Common.logger("brand:"+ _brand)
        brand = _brand.substring(0, _brand.length() - 1)
      }
    }
    brand.toLowerCase()
  }
  }
  private val getDeviceInfo = udf { (deviceInfo: String) => {
    var re = deviceInfo
    if (null == deviceInfo) {
      re = ""
    }
    re
  }

  }
  var getField = udf { (field1: String, field2: String) => {
    var re = field1
    if ((field1 == null || field1.trim.equalsIgnoreCase("")) &&
      (field2 != null && !field2.equalsIgnoreCase(""))) {
      re = field2
    }
    re
  }
  }

  def getBrandName(spark: SparkSession, game_code: String, logDate: String): DataFrame = {
    var path = s"/ge/gamelogs/sdk/$logDate/[game_code]_Login_InfoLog/AUM_Login_InfoLog-$logDate.gz"
    path = path.replace("[game_code]", game_code.toUpperCase)
    //    Common.logger("path: " + path)
    var df = spark.read.json(path)
    var fields: Array[String] = Array()
    df.schema.foreach { col =>
      fields = fields ++ Array(col.name)
    }

    if (!fields.contains("device_info")) {
      //      Common.logger("vao day")
      df = df.withColumn("device_info", lit(""))
    }
    df = df.withColumn("device_info", getDeviceInfo(col("device_info")))
    df = df.select("device_info", "device_os", "userID", "device")
    //    df.show()
    df = df.withColumn("brand", getBrand(col("device_info"), col("device_os")))
    df
  }

  def checkRevGnm(spark: SparkSession): Unit = {
    var sdkDf = spark.read.option("header", true).option("delimiter", "\\t").csv("/tmp/quangctn/gnm/doixoat_thang12")
    var df09 = sdkDf.where("ORDER_DATE like '%12/9/2017%'")
    var parseDf = spark.read.parquet("/ge/warehouse/gnm/ub/data/payment_2/2017-12-09/").select("log_date", "sid", "id", "net_amt")
    var join = df09.as('a).join(parseDf.as('b), df09("ROLE_ID") === parseDf("rid"), "left_outer")
    join.select("ORDER_DATE", "AMOUNT", "UID", "SERVER_ID", "b.*").where("log_date is null").orderBy(desc("ORDER_DATE")).show(376)
    //    5cf45ac7-75cb-45e6-a94f-4c4d3a5a8f28
    join.select("ORDER_DATE", "AMOUNT", "UID", "SERVER_ID", "b.*").where("log_date is null").withColumn("SERVER_ID", col("SERVER_ID").cast("long")).groupBy("SERVER_ID").agg(sum("AMOUNT").cast("long")).orderBy("SERVER_ID").show(1000)

  }
  /*def dxGnm_01_2018(spark: SparkSession): Unit ={
    val a = spark.read.parquet("/ge/warehouse/gnm/ub/data/payment_2/2018-01-31").select("log_date","id","rid","net_amt","gross_amt","sid").orderBy("log_date")
    var b = spark.read.option("header",true).option("delimiter","\\t").csv("/tmp/quangctn/gnm/dx_31012018.csv").orderBy("ORDER_DATE")
    /*var join = b.as('b).join(a.as('a),b("ROLE_ID") === a("rid"),"left_outer").select(b."ROLE_ID","b.SERVER_ID","b.AMOUNT","a.rid","a.sid","a.net_amt")*/
    join.where("a.rid is null").show

  }*/

  def mapDeviceName_Brand(device: String): String = {
    var device_name = device.toLowerCase

    var brand = ""
    if (device_name.startsWith("SM".toLowerCase())
      || device_name.startsWith("SAMSUNG".toLowerCase())
      || device_name.startsWith("GT".toLowerCase())
      || device_name.startsWith("SC".toLowerCase())
      || device_name.startsWith("SGH".toLowerCase())
      || device_name.startsWith("SHV".toLowerCase())) {
      brand = "samsung".toLowerCase()
    } else {
      if (device_name.contains("vivo")) {
        brand = "vivo".toLowerCase()
      }
      else {
        if (device_name.contains("asus".toLowerCase())) {
          brand = "asus".toLowerCase()
        }
        else {
          if (device_name.contains("mobiistar".toLowerCase())) {
            brand = "mobiistar".toLowerCase()
          }
          else {
            if (device_name.startsWith("LG".toLowerCase())) {
              brand = "lge".toLowerCase()
            }
            else {
              if (device_name.startsWith("Lenovo".toLowerCase())) {
                brand = "Lenovo".toLowerCase()
              }
              else {
                if (device_name.startsWith("".toLowerCase())) {
                  brand = "vega".toLowerCase()
                }
                else {
                  if (device_name.startsWith("Masstel".toLowerCase())) {
                    brand = "Masstel".toLowerCase()
                  } else {
                    if (device_name.startsWith("Coolpad".toLowerCase())) {
                      brand = "Coolpad".toLowerCase()
                    }
                    else {
                      if (device_name.startsWith("FPT".toLowerCase())) {
                        brand = "FPT".toLowerCase()
                      }
                    }
                  }

                }
              }
            }
          }
        }

      }
    }
    brand
  }
}




/*
dt2
spark.read.option("header",true).csv("/tmp/quangctn/dt2/dt2_export").where("createTime <'2017-12-02'")
spark.read.option("header",true).csv("/tmp/quangctn/dt2/dt2_export").where("createTime <'2017-12-02' and payMoney!=0")
.withColumn("game_code",lit("dt2"))
.selectExpr("game_code","createTime as log_date","accountId as id")
.coalesce(1).write.mode("overwrite").format("parquet").save("/ge/fairy/warehouse/dt2/ub/data/total_paid_acc_2/2017-12-01")*/

