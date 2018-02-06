package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.utils.DateTimeUtils
/**
  * Created by quangctn on 15/08/2017.
  * MRTG CODE : 299
  */
class Kvm extends FairyFormatter("kvm") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }
  //get 1 day
  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    val newDate = DateTimeUtils.getDate(logDate,1)
    var loginRaw: DataFrame = null
    if (hourly == "") {
      val loginPattern = Constants.GAME_LOG_DIR + "/kvm/[yyyy-MM-dd]/datalog/LOG_*/roleLoginLogout.txt"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, newDate, 1)
      loginRaw = getCsvWithHeaderLog(loginPath,"\t")
        .select("user_id","srv_id","login_time","online_time","ip","channel_id")
    }
    val sf= Constants.FIELD_NAME
    loginRaw = loginRaw
      .withColumn(sf.LOG_DATE,date_format(col("login_time").cast("double").cast("timestamp"),"yyyy-MM-dd HH:mm:ss"))
      .withColumnRenamed("srv_id",sf.SID)
      .withColumnRenamed("user_id",sf.ID)
      .withColumn(sf.GAME_CODE,lit("kvm"))
      .withColumnRenamed("channel_id",sf.CHANNEL)
    .withColumnRenamed("online_time",sf.ONLINE_TIME).withColumnRenamed("ip",sf.IP)
    .where(s"log_date like '%$logDate%'")
    loginRaw
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val newDate = DateTimeUtils.getDate(logDate,1)
    var paymentRaw: DataFrame = null
    if (hourly == "") {
      val paymentPattern = Constants.GAME_LOG_DIR + "/kvm/[yyyy-MM-dd]/datalog/LOG_*/recharge.txt"
      val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, newDate, 1)
      paymentRaw = getCsvWithHeaderLog(paymentPath,"\t")
        .select("user_id","srv_id","pay_time","is_succ","money","channel_id","order_num")
    }
    val sf= Constants.FIELD_NAME
    paymentRaw = paymentRaw
      .withColumn(sf.LOG_DATE,date_format(col("pay_time").cast("double").cast("timestamp"),"yyyy-MM-dd HH:mm:ss"))
      .withColumnRenamed("srv_id",sf.SID)
      .withColumnRenamed("user_id",sf.ID)
    .withColumnRenamed("money",sf.GROSS_AMT)
    .withColumn(sf.NET_AMT,col(sf.GROSS_AMT))
    .withColumnRenamed("channel_id",sf.CHANNEL)
    .withColumnRenamed("order_num",sf.TRANS_ID).withColumn(sf.GAME_CODE,lit("kvm"))
    .where(s"log_date like '%$logDate%' and is_succ == '1'")
    paymentRaw
  }
}
