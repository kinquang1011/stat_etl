package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils
import vng.ge.stats.etl.transform.adapter.myplay._
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.functions._

/**
  * Created by quangctn on 22/05/2017.
  */

/**
  * 1.Lieng Thai
  * 2.Poker Thai
  * 3.Dummy Thai
  * 4.Binh Indo
  * 5.Pokerus Indo
  * 6.Bida Mobile Sea
  * 7.Thoi Loan Sea
  * 8.Co ty phu Sea
  * 9.Farmery Sea
  * 10.Big 2 Indo
  */
class Myplay_International extends MyplayFormatter("myplay_international") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  private val liengTh = new Zingplaythai_Lieng
  private val pokerTh = new Zingplaythai_Poker
  private val binhIn = new Zpimgsn_Binh
  private val pokerusIn = new Zpimgsn_Pokerus
  private val bidamSea = new Myplay_Bidamobile_Sea
  private val coccmSea = new Myplay_Coccmsea
  private val ctpSea = new Myplay_Ctpsea
  private val farmerySea = new Myplay_FarmerySea
  private val big2 = new Zpimgsn_Big2
  private val dummy = new Zingplaythai_Dummy

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    val liengThDs = liengTh.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val pokerThDs = pokerTh.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val dummyThDs = dummy.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    val binhInDs = binhIn.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val pokerusInDs = pokerusIn.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val bidamSeaDs = bidamSea.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val coccmSeaDs = coccmSea.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val ctpSeaDs = ctpSea.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val farmerySeaDs = farmerySea.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val big2Ds = big2.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)

    var ds = liengThDs.union(pokerThDs).union(dummyThDs)
      .union(pokerusInDs).union(binhInDs).union(big2Ds)
      .union(bidamSeaDs).union(coccmSeaDs).union(ctpSeaDs).union(farmerySeaDs)
    ds = ds.withColumn(sf.GAME_CODE,lit("myplay_international"))
    ds
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/" +
        "{Cash_liengsea," +
        "Cash_poker_thailan," +
        "Cash_pokerus," +
        "Cash_big2," +
        "Cash_bidamobile_id,Cash_bidamobile_mm,Cash_bidamobile_th,Cash_bidamobile_my,Cash_bidamobile_inter," +
        "Cash_ThoiloanMobileSea," +
        "Cash_cotyphuSea," +
        "Cash_FarmerySea}-*"
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
      ("myplay_international", dateTime, id, gross_amt, net_amt, firstCharge)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT, sf.FIRST_CHARGE)
    val binhIn = new Zpimgsn_Binh
    val pymBinhInDs = binhIn.getPaymentDs(logDate, "").withColumn(sf.FIRST_CHARGE, lit(0))
      .select(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT, sf.FIRST_CHARGE)
    var ds = paymentDs.union(pymBinhInDs)
    ds = ds.withColumn(sf.GAME_CODE,lit("myplay_international"))
    ds
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
        && (line(0).toLowerCase.trim.equalsIgnoreCase("myplay_bidamobile")
        || line(0).toLowerCase.trim.equalsIgnoreCase("zptgsn_lieng_all")
        || line(0).toLowerCase.trim.equalsIgnoreCase("zptgsn_poker_total")
        || line(0).toLowerCase.trim.equalsIgnoreCase("zpimgsn_binh")
        || line(0).toLowerCase.trim.equalsIgnoreCase("zpimgsn_pokerus")
        || line(0).toLowerCase.trim.equalsIgnoreCase("zpimgsn_big2")
        || line(0).toLowerCase.trim.equalsIgnoreCase("ctpsea")
        || line(0).toLowerCase.trim.equalsIgnoreCase("farmerymobilesea")
        || line(0).toLowerCase.trim.equalsIgnoreCase("tlmsea")
        )) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    var ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
      val name = line(0)
      val ccu = line(1)
      val timeStamp = line(2).toLong * 1000
      val dateTime = MyUdf.timestampToDate(timeStamp)
      ("myplay_international", ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
    ccuDs = ccuDs.dropDuplicates(sf.GAME_CODE, sf.LOG_DATE)
    ccuDs
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    val sf = Constants.FIELD_NAME
    val regDs = getHiveRegisterDs(logDate)
    val bidaMSeaDs = bidamSea.getIdRegisterDs(logDate)
    val pokerIn = pokerusIn.getActivityDs(logDate,"")
    val regPokerIn = getMyplayIdRegisterDs(logDate,pokerIn,"zpimgsn_pokerus")
    val bigInDs = big2.getActivityDs(logDate,"")
    val regBigIn = getMyplayIdRegisterDs(logDate,bigInDs,"zpimgsn_big2")
    var resultDs = regDs.union(regPokerIn).union(regBigIn).dropDuplicates(sf.ID)
    resultDs = resultDs.withColumn(sf.GAME_CODE,lit("myplay_international"))
    resultDs
  }


}
