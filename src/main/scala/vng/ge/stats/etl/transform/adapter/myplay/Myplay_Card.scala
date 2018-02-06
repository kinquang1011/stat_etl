package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils
import vng.ge.stats.etl.transform.adapter.myplay._
import org.apache.spark.sql.functions._

/**
  * Created by quangctn on 22/05/2017.
  */

/**
  * 1.Bacay
  * 2.Binh
  * 3.PokerHK
  * 4.Sam
  * 5.Tala
  * 6.Tien Len
  * 7.Xi Dzach
  */
class Myplay_Card extends MyplayFormatter("myplay_card") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  private val bacay = new Myplay_Bacay
  private val binh = new Myplay_Binh
  private val pokerhk = new Myplay_Pokerhk
  private val sam = new Myplay_Sam
  private val tala = new Myplay_Tala
  private val tienlen = new Myplay_Tienlen
  private val xidzach = new Myplay_Xidzach

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    //BA CAY
    val bacayDs = bacay.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //BINH
    val binhDs = binh.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //POKER HK
    val pokerhkDs = pokerhk.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //SAM
    val samDs = sam.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //TALA
    val talaDs = tala.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //TIENLEN
    val tienlenDs = tienlen.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //XI DZACH
    val xidzachDs = xidzach.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    var ds = bacayDs.union(binhDs)
      .union(pokerhkDs)
      .union(samDs)
      .union(talaDs)
      .union(tienlenDs)
      .union(xidzachDs)
    ds = ds.withColumn(sf.GAME_CODE,lit("myplay_card"))
    ds
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/{Cash_BaCay,Cash_binh," +
        "Cash_poker,Cash_sam,Cash_tala,Cash_tienlen}-*"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath, logDate)
      paymentRaw = getRawLog(paymentPath, true, "\\|")
    }
    val date = logDate.replaceAll("-", "")
    val logFilter = (arr: Array[String]) => {
      var rs = false
      if (arr(0).contains(date)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\|")).filter(line => logFilter(line)).map { line =>
      val paymentName = MyUdf.getPaymentName(line(0), date)
      val data: String = line(1)
      val r: Array[String] = data.split("\\t")
      val dateTime = r.apply(6)
      val gross_amt = r.apply(4)
      val id = r.apply(2)
      val rate = r.apply(9).toDouble
      val net_amt = gross_amt.toDouble * rate
      val firstCharge = r.apply(8)
      ("myplay_card", dateTime, id, gross_amt, net_amt, firstCharge)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT, sf.FIRST_CHARGE)
    paymentDs
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null

    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/mrtg/server_ccu.[yyyyMMdd]"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
      raw = getRawLog(logPath)
    }
    val gameFilter = (line: Array[String]) => {
      var rs = false
      if (line.length >= 3
        && (line(0).toLowerCase.startsWith("myplay_bacay")
        || line(0).toLowerCase.startsWith("myplay_binh_new")
        || line(0).toLowerCase.trim.equalsIgnoreCase("myplay_tienlen_new")
        || line(0).toLowerCase.startsWith("myplay_pokerhk")
        || line(0).toLowerCase.startsWith("myplay_xidzach")
        || line(0).toLowerCase.startsWith("myplay_tala_new")
        || line(0).toLowerCase.startsWith("myplay_sam")
        )) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
      val name = line(0)
      val ccu = line(1)
      val timeStamp = line(2).toLong * 1000
      val dateTime = MyUdf.timestampToDate(timeStamp)
      ("myplay_card", ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
    ccuDs
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    val sf = Constants.FIELD_NAME
    val binhDs = binh.getIdRegisterDs(logDate)
    val pokerhkDs = pokerhk.getIdRegisterDs(logDate)
    val samDs = sam.getIdRegisterDs(logDate)
    val talaDs = tala.getIdRegisterDs(logDate)
    val tienlenDs = tienlen.getIdRegisterDs(logDate)
    //Non-existed folder register
    val bacayA1 = bacay.getActivityDs(logDate,"")
    val bacayDs = getMyplayIdRegisterDs(logDate,bacayA1,"myplay_bacay")
    val xidzachA1 = xidzach.getActivityDs(logDate,"")
    val xidzachDs = getMyplayIdRegisterDs(logDate,xidzachA1,"myplay_xidzach")


    val ds = bacayDs.union(binhDs)
      .union(pokerhkDs)
      .union(samDs)
      .union(talaDs)
      .union(tienlenDs)
      .union(xidzachDs)
    ds.dropDuplicates(sf.ID).withColumn(sf.GAME_CODE,lit("myplay_card"))
  }


}
