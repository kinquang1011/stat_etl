package vng.ge.stats.etl.transform.adapter


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf


/**
  * Created by quangctn on 13/02/2017.
  */
class Hpt extends FairyFormatter("hpt") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var logUserFromHive: DataFrame = null
    if (hourly == "") {
      val logQuery = s"select * from hpt.ccu where ds = '$logDate'"
      logUserFromHive = getHiveLog(logQuery)
    }
    val sf = Constants.FIELD_NAME
    var logDs = logUserFromHive.selectExpr("log_date as date", "online as ccu", "server_id as sid","opr_id as os")
    logDs = logDs.withColumn("game_code", lit("hpt"))
    logDs = logDs.withColumn(sf.LOG_DATE,MyUdf.roundCCUTime(col("date")))
    logDs
  }

}
