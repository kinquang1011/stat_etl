package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils
import org.apache.spark.sql.functions._


/**
  * Created by quangctn on 18/05/2017.
  */

/**
  * 1.Co ti phu
  * 2.Farmery : SuperFarm + Farmery Web
  * 3.Bida
  * 4.Bida Mobile
  * 5.Bida Card
  * 6.Co ca ngua
  * 7.Co tuong
  * 8.Co up Close
  * 9.Thoi loan
  * 10. Caro
  *
  */
class Myplay_Noncard extends MyplayFormatter("myplay_noncard") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  private val ctp = new Myplay_Ctpgsn
  private val superFarm = new Myplay_Superfarm
  private val webFarm = new Myplay_Sfgsn
  private val bida = new Myplay_Bida
  private val bidaM = new Myplay_Bidamobile
  private val bidaC = new Myplay_Bidacard
  private val ccn = new Myplay_Ccn
  private val cotuong = new Myplay_Cotuong
  /*private val coup = new Myplay_Coup*/
  private val coccgsn = new Myplay_Coccgsn
  private val coccm = new Myplay_Coccmgsn
  private val caro = new Myplay_Caro

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    //TYPHU
    val ctpDs = ctp.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //FARMERY
    val superFarmDs = superFarm.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val webFarmDs = webFarm.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val farmeryDs = superFarmDs.union(webFarmDs)
    //BIDA
    val bidaDs = bida.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //BIDA_MOBILE
    val bidaMDs = bidaM.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //BIDA_CARD
    val bidaCDs = bidaC.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //CCN
    val ccnDs = ccn.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //CO TUONG
    val cotuongDs = cotuong.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //CO UP
    /*val coupDs = coup.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)*/
    //THOI LOAN
    val coccgsnDs = coccgsn.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val coccmDs = coccm.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    val thoiloanDs = coccgsnDs.union(coccmDs)
    //CARO
    val caroDs = caro.getActivityDs(logDate, "").select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    //UNION
    var ds = ctpDs
      .union(bidaDs)
      .union(bidaMDs)
      .union(bidaCDs)
      .union(ccnDs)
      .union(cotuongDs)
      /*.union(coupDs)*/
      .union(farmeryDs)
      .union(thoiloanDs)
      .union(caroDs)
    ds = ds.withColumn(sf.GAME_CODE,lit("myplay_noncard"))
    ds
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/{Cash_cotyphu,Cash_SuperFarm," +
        "Cash_Farmery2,Cash_bida9bi,Cash_bidamobile_vi,Cash_bidacard,Cash_cotuong,Cash_COC,Cash_ThoiLoanMobile_vi}-*"
      //Cash_coup
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
      ("myplay_noncard", dateTime, id, gross_amt, net_amt, firstCharge)
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
        && (line(0).toLowerCase.startsWith("myplay_cotuongnew")
        || line(0).toLowerCase.startsWith("myplay_cangua")
       /* || line(0).toLowerCase.startsWith("myplay_coup")*/
        || line(0).toLowerCase.startsWith("cotiphu_1")
        || line(0).toLowerCase.startsWith("myplay_bidamobile")
        || line(0).toLowerCase.startsWith("farmery_all")
        || line(0).toLowerCase.startsWith("sfgsn")
        || line(0).toLowerCase.startsWith("thoiloan")
        || line(0).toLowerCase.startsWith("coccmgsn")
        || line(0).toLowerCase.startsWith("myplay_bidacard")
        || line(0).toLowerCase.startsWith("myplay_bida")
        || line(0).toLowerCase.startsWith("myplay_caronew")
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
      ("myplay_noncard", ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.CCU, sf.LOG_DATE)
    ccuDs
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    val sf = Constants.FIELD_NAME

    val ctpDs = ctp.getIdRegisterDs(logDate)
    val superFarmDs = superFarm.getIdRegisterDs(logDate)
    val webFarmDs = webFarm.getIdRegisterDs(logDate)
    val farmeryDs = superFarmDs.union(webFarmDs)
    val bidaMDs = bidaM.getIdRegisterDs(logDate)
    val cotuongDs = cotuong.getIdRegisterDs(logDate)
  /*  val coupDs = coup.getIdRegisterDs(logDate)*/
    val coccgsnDs = coccgsn.getIdRegisterDs(logDate)
    val coccmDs = coccm.getIdRegisterDs(logDate)
    val thoiloanDs = coccgsnDs.union(coccmDs)
    //non existed folder register
    val bidaA1 = bida.getActivityDs(logDate,"")
    val bidaCA1 = bidaC.getActivityDs(logDate,"")
    val caroA1 = caro.getActivityDs(logDate,"")
    val ccnA1 = ccn.getActivityDs(logDate,"")

    val bidaDs = getMyplayIdRegisterDs(logDate,bidaA1,"myplay_bida")
    val bidaCDs = getMyplayIdRegisterDs(logDate,bidaCA1,"myplay_bidacard")
    val caroDs =  getMyplayIdRegisterDs(logDate,caroA1,"myplay_caro")
    val ccnDs = getMyplayIdRegisterDs(logDate,ccnA1,"myplay_ccn")
    //UNION
    val ds = ctpDs
      .union(bidaDs)
      .union(bidaMDs)
      .union(bidaCDs)
      .union(ccnDs)
      .union(cotuongDs)
     /* .union(coupDs)*/
      .union(farmeryDs)
      .union(thoiloanDs)
      .union(caroDs)
    ds.dropDuplicates(sf.ID).withColumn(sf.GAME_CODE,lit("myplay_noncard"))
  }


}
