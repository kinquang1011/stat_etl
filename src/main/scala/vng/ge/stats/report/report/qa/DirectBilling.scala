package vng.ge.stats.report.report.qa

/**
  * Created by canhtq on 15/05/2017.
  */

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.db.MysqlDB
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.utils.Common
import vng.ge.stats.report.sql.DbMySql
import vng.ge.stats.report.util.IdConfig

import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.util.Try


object DirectBilling {

  val newGameTableName = "new_games"
  val mysqlDB = new MysqlDB()
  val tableName = "qc_game_kpi"
  val currentDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dataSource = "dbg"
  var mysql: DbMySql = new DbMySql()
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
        if(line.length>33){
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
        val net_amt = line(33)
        val log_date = line(23)
        (transid,log_date, id, sid, appid, net_amt)
      }.toDF("transid","log_date", "id", "sid", "appid", "net_amt")
      //dbgDs = spark.read.option("delimiter","\t").csv(dbgDatePath).selectExpr("_c0 as transid","_c2 as id", "_c23 as log_date","_c1 as appid", "_c33 as net_amt")
      cnfDs = spark.read.option("delimiter",",").option("header","true").csv(dbgAppCnfPath)
    }
    val a = "a".toLowerCase()
    Common.logger("DailyProcessing")
    if (bpm.isSuccess) {
      Common.logger("QASTART===============")
      dbgDs.createOrReplaceTempView("dbgDs")
      cnfDs.createOrReplaceTempView("cnfDs")
      payment = spark.sql("select dbgDs.*, cnfDs.game_code from dbgDs, cnfDs where dbgDs.appid = cnfDs.appid").withColumn("hourly",hour(col("log_date")))
      val revResult = payment.selectExpr("game_code", "net_amt","id").groupBy("game_code").agg(sum("net_amt").cast("long").as("net_amt"),countDistinct("id").cast("long").as("pu"))
      //16001
      val revKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.NET_REVENUE, "a1").toString
      val puKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.PAYING_USER, "a1").toString
      val now = Calendar.getInstance().getTime()
      val currentDate = currentDateFormat.format(now)
      cleanOldData(logDate,dataSource,revKpiId,tableName)
      cleanOldData(logDate,dataSource,puKpiId,tableName)
      var revResults = List[LinkedHashMap[String, Any]]()
      val sql = getInsertSql(tableName)

      revResult.coalesce(1).collect.foreach { line =>
        val gc = line.getAs[String]("game_code")
        val rev = line.getAs[Long]("net_amt")
        val pu = line.getAs[Long]("pu")
        //storeKpiDb(logDate, gc, dataSource, revKpiId, rev.toString, currentDate, tableName)
        //storeKpiDb(logDate, gc, dataSource, puKpiId, pu.toString, currentDate, tableName)
        var row = LinkedHashMap[String, Any]()
        row += ("report_date" -> logDate)
        row += ("game_code" -> gc)
        row += ("source" -> dataSource)
        row += ("kpi_id" -> revKpiId)
        row += ("kpi_value" -> rev.toString)
        row += ("calc_date" -> currentDate)

        revResults ++= List(row)
      }
      if(revResults.length>0){
        mysql.excuteBatchInsert(sql,revResults)
      }

      Common.logger("QAEND===============")
    }


    mysqlDB.close()
    sc.stop()
    Common.logger("Shutdown")
  }
  def getInsertSql(tableName: String): String = {

    val sql = "insert into " + tableName + " (report_date, game_code, source, kpi_id, kpi_value,calc_date) values(?,?,?,?,?,?)"
    sql
  }

  def cleanOldData(logDate: String,  dataSource: String, kpiId: String, tableName: String): Unit = {
    val sqlDelete = "delete from " + tableName + " where  "+
      " source = '" + dataSource + "'" +
      " and report_date = '" + logDate + "'" +
      " and kpi_id = " + kpiId
    mysql.executeUpdate(sqlDelete, null)
  }

}