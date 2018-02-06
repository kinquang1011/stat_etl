package vng.ge.stats.etl.transform.adapter

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.Common


/**
  * Created by quangctn on 08/02/2017.
  */
class FlumeCcu extends FairyFormatter("flumeccu") {

  def start(args: Array[String]): Unit = {
    var logDate: String = ""
    var codeGame: String = ""
    for (x <- args) {
      val xx = x.split("=")
      if (xx(0).contains("logDate")) {
        logDate = xx(1)
      }

    }
    Common.logger("para 1-logDate: " + logDate)

    this -> otherETL(logDate, "", "") -> close
  }

  def getCcuDs(logDate: String, spark: SparkSession): DataFrame = {
    import sqlContext.implicits._
    val sf = Constants.FIELD_NAME
    var ccu2: DataFrame = null
    val ccu = spark.read.json(s"/ge/gamelogs/mrtg/flume_ccu_result_$logDate.log")
      .select(explode(col("group_server"))).select("col.*")
    val ccuRaw = ccu.withColumn("server_list", explode($"server_list"))
      .select("unixtime", "server_list", "cmdb_prd_code", "product_name").map { line => line(0) + "," + line(1) + "," + line(2) + "," + line(3) }.rdd
    ccu2 = ccuRaw.map(line => line.split(",")).map { r =>
      val timeStamp = MyUdf.timestampToDate((r(0)).toLong * 1000)
      val ccuStr = (r(1)).replace("[", "")
      val sid = r(3)
      val cmdb_prd_code = r(r.length - 2)
      val name = r(r.length - 1)
      (cmdb_prd_code, timeStamp, ccuStr, sid, name)
    }.toDF("cmdb_prd_code", sf.LOG_DATE, sf.CCU, sf.SID, "name")
    ccu2 = ccu2.selectExpr("cmdb_prd_code", sf.LOG_DATE, "cast(ccu as long)", sf.SID, "name").dropDuplicates("cmdb_prd_code", "log_date", "ccu", "sid", "name")
    /*val pathCcu = "/ge/fairy/master_data/gamecode_ccu.csv"
    Common.logger("Path file: " + pathCcu)
    val df = spark.read.option("header", true).csv(pathCcu)
    var join = ccu2.as('a).join(df.as('b), ccu2("cmdb_prd_code") === df("codeCcu"), "left_outer").where("codeCcu is not null")
    join = join.withColumnRenamed("codeGame", sf.GAME_CODE).drop("codeCcu", "cmdb_prd_code")
    join*/
    ccu2
  }

  //Generate parquet file to folder ge/fairy/warehouse/flumeccu
  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
    var ccuW: DataFrame = null
    ccuW = getCcuDs(logDate, sparkSession)
    val pathStoreCcu = Constants.FAIRY_WAREHOUSE_DIR + s"/flumeccu/$logDate"
    Common.logger("Path store ccu: " + pathStoreCcu)
    ccuW.printSchema()
    ccuW.coalesce(1).write.mode("overwrite").parquet(pathStoreCcu)

  }
}
