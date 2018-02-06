package vng.ge.stats.etl.adhoc

import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.db.MysqlDB
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.transform.udf.MyUdf.udfSdkLoginGameId
import vng.ge.stats.etl.utils.Common
import vng.ge.stats.report.util.IdConfig

import scala.util.Try

/**
  * Created by canhtq on 23/02/2017.
  */

object SdkReportHourly {

    val TIMMING: Map[String, String] = Map(
        "4" -> "1",
        "5" -> "7",
        "6" -> "30"
    )
    val mysqlDB = new MysqlDB()
    val tableHourly = "nrt_game_kpi"
    val currentDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dataSource = "sdk"

    var gameCodeInput = ""
    var serverReport = "false"
    var rootDir = "/ge/fairy/warehouse"

    val spark = SparkEnv.getSparkSession

    def main(args: Array[String]): Unit = {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        if (mapParameters.contains("rootDir")) {
            rootDir = mapParameters("rootDir")
        }
        val sc = SparkEnv.getSparkContext
        val logDate = mapParameters("logDate")
        val sdkDatePath =  "/ge/gamelogs/sdk/" + logDate +"/Log_*_DBGAdd"
        val sdkDateLoginPath =  "/ge/gamelogs/sdk/" + logDate +"/*_Login_InfoLog/*_Login_InfoLog-" + logDate +"*"
        val dbgAppCnfPath =  "/ge/fairy/master_data/mto_sdk_games.csv"

        Common.logger("data input: " + sdkDatePath)

        var payment: DataFrame = null
        var active: DataFrame = null
        var cnfDs: DataFrame = null
        var dbgDs: DataFrame = null
        var loginDs: DataFrame = null
        var bpm = Try {
            dbgDs = spark.read.json(sdkDatePath).selectExpr("transactionID as transid","userID as id", "updatetime as log_date","lower(gameID) as game_id", "pmcNetChargeAmt as net_amt")
            loginDs = spark.read.json(sdkDateLoginPath).selectExpr("userID as id", "updatetime as log_date").withColumn("game_id",udfSdkLoginGameId(input_file_name()))

            cnfDs = spark.read.option("delimiter",",").option("header","true").csv(dbgAppCnfPath)
        }
        val a = "a".toLowerCase()
        Common.logger("hourly-rev")
        if (bpm.isSuccess) {
            Common.logger("Gross revenue start")
            dbgDs.createOrReplaceTempView("dbgDs")
            loginDs.createOrReplaceTempView("loginDs")
            cnfDs.createOrReplaceTempView("cnfDs")
            payment = spark.sql("select dbgDs.*, cnfDs.game_code from dbgDs, cnfDs where dbgDs.game_id = cnfDs.game_id").withColumn("hourly",hour(col("log_date")))
            payment.cache()
            val hourlyRevResult = payment.selectExpr("hourly", "game_code", "net_amt").groupBy("game_code", "hourly").agg(sum("net_amt").cast("long").as("net_amt")).orderBy("game_code", "hourly")
            //16001
            val revKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.NET_REVENUE, "hourly").toString
            val now = Calendar.getInstance().getTime()
            val currentDate = currentDateFormat.format(now)
            cleanHourlyData(logDate,dataSource,revKpiId,tableHourly)
            hourlyRevResult.coalesce(1).collect.foreach { line =>
                val gc = line.getAs[String]("game_code")
                val rev = line.getAs[Long]("net_amt")
                val hh = line.getAs[Int]("hourly")
                storeKpiDb(logDate, gc, dataSource, revKpiId, rev.toString, currentDate, tableHourly, hh.toString)

            }
            val puKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.PAYING_USER, "hourly").toString
            val hourlyPuResult = payment.selectExpr("hourly", "game_code", "id").groupBy("game_code", "hourly").agg(countDistinct("id").cast("long").as("pu")).orderBy("game_code", "hourly")
            cleanHourlyData(logDate,dataSource,puKpiId,tableHourly)
            hourlyPuResult.coalesce(1).collect.foreach { line =>
                val gc = line.getAs[String]("game_code")
                val pu = line.getAs[Long]("pu")
                val hh = line.getAs[Int]("hourly")
                storeKpiDb(logDate, gc, dataSource, puKpiId, pu.toString, currentDate, tableHourly, hh.toString)
            }
            Common.logger("Paying user end")

            active = spark.sql("select loginDs.*, cnfDs.game_code from loginDs, cnfDs where loginDs.game_id = cnfDs.game_id").withColumn("hourly",hour(col("log_date")))
            val activeKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.ACTIVE, "hourly").toString

            //val activeResult = active.selectExpr("game_code", "id").groupBy("game_code").agg(countDistinct("id").cast("long").as("active")).orderBy("game_code")
            val activeResult = active.selectExpr("hourly", "game_code", "id").groupBy("game_code", "hourly").agg(countDistinct("id").cast("long").as("active")).orderBy("game_code", "hourly")
            cleanHourlyData(logDate,dataSource,activeKpiId,tableHourly)
            activeResult.coalesce(1).collect.foreach { line =>
                val gc = line.getAs[String]("game_code")
                val active = line.getAs[Long]("active")
                val hh = line.getAs[Int]("hourly")
                storeKpiDb(logDate, gc, dataSource, activeKpiId, active.toString, currentDate, tableHourly, hh.toString)
            }
        }


        if (payment != null) {
            payment.unpersist()
        }
        mysqlDB.close()
        sc.stop()
        Common.logger("Shutdown")
    }

    def storeKpiDb(logDate: String, gameCode: String, dataSource: String, kpiId: String, valueString: String, current_date: String, tableName: String, hour: String): Unit = {
        val sql = "insert into " + tableName + " (report_date, game_code, source, kpi_id, kpi_value,hour,calc_date) values(" +
          "'" + logDate + "','" + gameCode + "','" + dataSource + "'," + kpiId + "," + valueString + "," + hour  + ",'" + current_date + "')"
        mysqlDB.executeUpdate(sql, false)
    }

    def cleanHourlyData(logDate: String,  dataSource: String, kpiId: String, tableName: String): Unit = {
        val sqlDelete = "delete from " + tableName + " where  "+
          " source = '" + dataSource + "'" +
          " and report_date = '" + logDate + "'" +
          " and kpi_id = " + kpiId
        mysqlDB.executeUpdate(sqlDelete, false)
    }

}