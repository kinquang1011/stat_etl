package vng.ge.stats.report.report.qa

import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.db.MysqlDB
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.transform.udf.MyUdf.{changeReportTime, udfSdkLoginGameId}
import vng.ge.stats.etl.utils.{Common, PathUtils}
import vng.ge.stats.report.sql.DbMySql
import vng.ge.stats.report.util.IdConfig

import scala.collection.mutable.LinkedHashMap
import scala.util.Try

/**
  * Created by canhtq on 23/02/2017.
  */

object MtoSdk {

    val mysqlDB = new MysqlDB()
    var mysql: DbMySql = new DbMySql()
    val tableName = "qc_game_kpi"
    val newGameTableName = "new_games"
    val currentDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var dataSource = "sdk"

    var gameCodeInput = ""
    var serverReport = "false"
    var logDir = "/ge/gamelogs/sdk"

    val spark = SparkEnv.getSparkSession

    def main(args: Array[String]): Unit = {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            val xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        if (mapParameters.contains("logDir")) {
            logDir = mapParameters("logDir")
        }
        dataSource = logDir.replace("/ge/gamelogs/","")
        val addTime = udf { (datetime: String, addedTime: String) => {
            MyUdf.dateTimeIncrement(datetime, addedTime.toInt * 3600)
        }}
        val sc = SparkEnv.getSparkContext
        val logDate = mapParameters("logDate")
        val sdkDBGPattern =  logDir+"/[yyyy-MM-dd]/Log_*_DBGAdd/Log_*_DBGAdd-[yyyy-MM-dd].gz"
        val sdkDateLoginPattern =  logDir +"/[yyyy-MM-dd]/*_Login_InfoLog/*_Login_InfoLog-[yyyy-MM-dd].gz"
        val dbgAppCnfPath =  "/ge/fairy/master_data/mto_sdk_games.csv"


        var payment: DataFrame = null
        var active: DataFrame = null
        var cnfDs: DataFrame = null
        var dbgDs: DataFrame = null
        var loginDs: DataFrame = null
        val bpm = Try {
            val sdkDBGPath = PathUtils.generateLogPathDaily(sdkDBGPattern, logDate)
            dbgDs = spark.read.json(sdkDBGPath:_*).selectExpr("transactionID as transid","userID as id", "updatetime","lower(gameID) as game_id", "pmcNetChargeAmt as net_amt")
            val sdkLoginPath = PathUtils.generateLogPathDaily(sdkDateLoginPattern, logDate)
            loginDs = spark.read.json(sdkLoginPath:_*).selectExpr("userID as id", "updatetime").withColumn("game_id",udfSdkLoginGameId(input_file_name()))
            cnfDs = spark.read.option("delimiter",",").option("header","true").csv(dbgAppCnfPath)
        }
        Common.logger("daily")
        if (bpm.isSuccess) {
            Common.logger("QASTART===============NEW")
            dbgDs.createOrReplaceTempView("dbgDs")
            loginDs.createOrReplaceTempView("loginDs")
            cnfDs.createOrReplaceTempView("cnfDs")
            payment = spark.sql("select dbgDs.*, cnfDs.game_code,cnfDs.added_time,cnfDs.change_rate from dbgDs, cnfDs where dbgDs.game_id = cnfDs.game_id").withColumn("log_date",addTime(col("updatetime"),col("added_time"))).withColumn("date",substring(col("log_date"),0,10)).where(s"date=='$logDate'")
            val revResult = payment.selectExpr("game_code", "net_amt","id","change_rate").groupBy("game_code").agg(sum("net_amt").cast("long").as("net_amt"),countDistinct("id").cast("long").as("pu"),first("change_rate").as("change_rate"))
            //16001
            val revKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.NET_REVENUE, "a1").toString
            val puKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.PAYING_USER, "a1").toString
            val now = Calendar.getInstance().getTime()
            val currentDate = currentDateFormat.format(now)
            cleanOldData(logDate,dataSource,revKpiId,tableName)
            cleanOldData(logDate,dataSource,puKpiId,tableName)
            var revResults = List[LinkedHashMap[String, Any]]()
            val sql = getInsertSql(tableName)
            //report_date, game_code, source, kpi_id, kpi_value,calc_date
            revResult.coalesce(1).collect.foreach { line =>
                val gc = line.getAs[String]("game_code")
                val rate = line.getAs[String]("change_rate").toFloat
                val rev = line.getAs[Long]("net_amt") * rate
                val pu = line.getAs[Long]("pu")
                var row = LinkedHashMap[String, Any]()
                row += ("report_date" -> logDate)
                row += ("game_code" -> gc)
                row += ("source" -> dataSource)
                row += ("kpi_id" -> revKpiId)
                row += ("kpi_value" -> rev.toString)
                row += ("calc_date" -> currentDate)

                revResults ++= List(row)


                var row2 = LinkedHashMap[String, Any]()
                row2 += ("report_date" -> logDate)
                row2 += ("game_code" -> gc)
                row2 += ("source" -> dataSource)
                row2 += ("kpi_id" -> puKpiId)
                row2 += ("kpi_value" -> pu.toString)
                row2 += ("calc_date" -> currentDate)

                revResults ++= List(row2)

                //storeKpiDb(logDate, gc, dataSource, revKpiId, rev.toString, currentDate, tableName)
                //storeKpiDb(logDate, gc, dataSource, puKpiId, pu.toString, currentDate, tableName)
                //sqlRev+=getInsertSql(logDate, gc, dataSource, revKpiId, rev.toString, currentDate, tableName)
                //sqlRev+=getInsertSql(logDate, gc, dataSource, puKpiId, pu.toString, currentDate, tableName)
            }
            if(revResults.length>0){
                mysql.excuteBatchInsert(sql,revResults)
            }
            active = spark.sql("select loginDs.*, cnfDs.game_code,cnfDs.added_time from loginDs, cnfDs where loginDs.game_id = cnfDs.game_id").withColumn("log_date",addTime(col("updatetime"),col("added_time"))).withColumn("date",substring(col("log_date"),0,10)).where(s"date='$logDate'")
            val activeKpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.ACTIVE, "a1").toString

            val activeResult = active.selectExpr("game_code", "id").groupBy("game_code").agg(countDistinct("id").cast("long").as("active"))
            cleanOldData(logDate,dataSource,activeKpiId,tableName)
            var aResults = List[LinkedHashMap[String, Any]]()
            activeResult.coalesce(1).collect.foreach { line =>
                val gc = line.getAs[String]("game_code")
                val active = line.getAs[Long]("active")
                //sqlActive+=getInsertSql(logDate, gc, dataSource, activeKpiId, active.toString, currentDate, tableName)
                //storeKpiDb(logDate, gc, dataSource, activeKpiId, active.toString, currentDate, tableName)

                var row = LinkedHashMap[String, Any]()
                row += ("report_date" -> logDate)
                row += ("game_code" -> gc)
                row += ("source" -> dataSource)
                row += ("kpi_id" -> activeKpiId)
                row += ("kpi_value" -> active.toString)
                row += ("calc_date" -> currentDate)
                aResults ++= List(row)
            }
            if(aResults.length>0){
                mysql.excuteBatchInsert(sql,aResults)
            }

            var nResults = List[LinkedHashMap[String, Any]]()
            val newDs = spark.sql("select loginDs.game_id, cnfDs.game_code from loginDs LEFT OUTER JOIN cnfDs ON loginDs.game_id = cnfDs.game_id").where("game_code is null").select("game_id").distinct()
            val newSql =getNewGameSql(newGameTableName)
            newDs.coalesce(1).collect.foreach { line =>
                val gc = line.getAs[String]("game_id")
                //setNewGame(logDate,gc,dataSource,newGameTableName)
                //newSql +=getNewGameSql(logDate,gc,dataSource,newGameTableName)
                var row = LinkedHashMap[String, Any]()
                row += ("report_date" -> logDate)
                row += ("game_code" -> gc)
                row += ("source" -> dataSource)
                nResults ++= List(row)

            }
            if(nResults.length>0){
                mysql.excuteBatchInsert(newSql,nResults)
            }

            Common.logger("QAEND===============")
        }else{
            Common.logger("QAFAILED !bpm.isSuccess")
        }


        if (payment != null) {
            payment.unpersist()
        }

        sc.stop()
        Common.logger("Shutdown")
    }

    def getInsertSql(tableName: String): String = {
        val sql = "insert into " + tableName + " (report_date, game_code, source, kpi_id, kpi_value,calc_date) values(?,?,?,?,?,?)"
        sql
    }

    def getNewGameSql(tableName:String): String = {
        val sql = "insert into " + tableName + " (report_date, game_code, source) values(?,?,?)"
        sql
    }


    def cleanOldData(logDate: String,  dataSource: String, kpiId: String, tableName: String): Unit = {
        val sqlDelete = "delete from " + tableName + " where  "+
          " source = '" + dataSource + "'" +
          " and report_date = '" + logDate + "'" +
          " and kpi_id = " + kpiId
        //mysqlDB.executeUpdate(sqlDelete, false)
        mysql.executeUpdate(sqlDelete,null)
    }
}