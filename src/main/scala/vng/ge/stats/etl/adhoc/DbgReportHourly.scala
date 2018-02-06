package vng.ge.stats.etl.adhoc

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.db.MysqlDB
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.utils.Common
import vng.ge.stats.report.util.IdConfig

import scala.collection.mutable
import scala.util.Try

/**
  * Created by canhtq on 23/02/2017.
  */

object DbgReportHourly {

    val TIMMING: Map[String, String] = Map(
        "4" -> "1",
        "5" -> "7",
        "6" -> "30"
    )
    val mysqlDB = new MysqlDB()
    val tableHourly = "nrt_game_kpi"
    val currentDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dataSource = "payment"

    var gameCodeInput = ""
    var serverReport = "false"
    var rootDir = "/ge/fairy/warehouse"

    val spark = SparkEnv.getSparkSession

    def main(args: Array[String]): Unit = {
        import  spark.implicits._
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
        val dbgDatePath =  "/ge/gamelogs/dbg/" + logDate
        val dbgAppCnfPath =  "/ge/fairy/master_data/directbilling_apps.csv"

        Common.logger("data input: " + dbgDatePath)

        var payment: DataFrame = null
        var cnfDs: DataFrame = null
        var dbgDs: DataFrame = null
        var bpm = Try {
            val appsFilter = (line: Array[String]) => {
                var rs = false
                if(line.length>26){
                    rs = line(26).equalsIgnoreCase("SUCCESSFUL")
                }else {
                    //Common.logger("line = " + line.mkString("###"))
                }
                rs
            }
            val raw = spark.read.text(dbgDatePath)
            dbgDs = raw.map(line => line.getString(0).split("\\t")).filter(line => appsFilter(line)).map { line =>
                val transid = line(0)
                val appid = line(1)
                val id = line(2)
                val sid = line(5)
                val gross_amt = line(15)
                val net_amt = line(16)
                val log_date = line(23)
                (transid,log_date, id, sid, appid, net_amt)
            }.toDF("transid","log_date", "id", "sid", "appid", "net_amt")

            //dbgDs = spark.read.option("delimiter","\t").csv(dbgDatePath).selectExpr("_c0 as transid","_c2 as id", "_c23 as log_date","_c1 as appid", "_c33 as net_amt")
            cnfDs = spark.read.option("delimiter",",").option("header","true").csv(dbgAppCnfPath)
        }
        val a = "a".toLowerCase()
        Common.logger("hourly-rev")
        if (bpm.isSuccess) {
            Common.logger("Gross revenue start")
            dbgDs.createOrReplaceTempView("dbgDs")
            cnfDs.createOrReplaceTempView("cnfDs")
            payment = spark.sql("select dbgDs.*, cnfDs.game_code from dbgDs, cnfDs where dbgDs.appid = cnfDs.appid").withColumn("hourly",hour(col("log_date")))
            payment.cache()
            //val paymentT = payment.groupBy("game_code").agg(sum("net_amt")).orderBy("game_code")
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

            val activeResult = payment.selectExpr("hourly", "game_code", "id").groupBy("game_code", "hourly").agg(countDistinct("id").cast("long").as("active")).orderBy("game_code", "hourly")
            val activeKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.ACTIVE, "hourly").toString
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