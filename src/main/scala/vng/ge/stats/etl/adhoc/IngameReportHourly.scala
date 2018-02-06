package vng.ge.stats.etl.adhoc

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.functions.{countDistinct, sum}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.db.MysqlDB
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.etl.utils.Common
import vng.ge.stats.etl.utils.DateTimeUtils
import vng.ge.stats.report.sql.DbMySql
import vng.ge.stats.report.util.IdConfig

import scala.collection.mutable
import scala.util.Try

/**
  * Created by tuonglv on 23/02/2017.
  */

object IngameReportHourly {

    val NO_DATA: String = "nodata"
    val TIMMING: Map[String, String] = Map(
        "4" -> "1",
        "5" -> "7",
        "6" -> "30"
    )
    val mysqlDB = new MysqlDB()
    var mysql: DbMySql = new DbMySql()
    val tableHourly = "game_kpi_hourly"
    val tableDaily = "realtime_game_kpi"
    val tableServerHourly = "server_kpi_hourly"
    val currentDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val oldGames: List[String] = List("tlbbmmmmmmmmmmmm")
    val outputPath = "/ge/warehouse/mtosdk/ub/sdk_report_hourly/a1"
    val dataSource = "ingame"

    var gameCodeInput = ""
    var serverReport = "false"
    var rootDir = "/ge/fairy/warehouse"

    val sparkSesion = SparkEnv.getSparkSession

    def main(args: Array[String]): Unit = {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }


        /* not support multiple games
        var gameList: List[String] = List()
        if (mapParameters.contains("gameList")) {
            gameList = mapParameters("gameList").split(",").toList
        }
        if (mapParameters.contains("gameCode")) {
            gameList = mapParameters("gameCode") :: gameList
        }
        */
        gameCodeInput = mapParameters("gameCode")
        var dataInput = "data_hourly"
        if (mapParameters.contains("dataInput")) {
            dataInput = mapParameters("dataInput").toString
        }
        var logDate = mapParameters("logDate")

        if (mapParameters.contains("serverReport")) {
            serverReport = mapParameters("serverReport")
        }

        if (mapParameters.contains("rootDir")) {
            rootDir = mapParameters("rootDir")
        }
        var rerunPrevDay = "";
        if (mapParameters.contains("rerunPrevDay")) {
            rerunPrevDay = mapParameters("rerunPrevDay")
        }
        if(rerunPrevDay.equalsIgnoreCase("yes")){
            //logDate = DateTimeUtils.getDateBefore(logDate,Constants.Timing.A1)
            dataInput= "data"
        }

        /*
        val conf = new SparkConf().setAppName("Ingame Report Hourly")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        */


        val sqlContext = SparkEnv.getSqlContext
        val sc = SparkEnv.getSparkContext

        import sqlContext.implicits._

        //val gameListString = gameList.mkString(",")
        //val preInputPath = "/ge/warehouse/{" + gameListString + "}/ub/"  + dataInput
        val preInputPath = rootDir + "/" + gameCodeInput + "/ub/" + dataInput
        Common.logger("data input: " + preInputPath)

        //16001
        var kpiId:String = IdConfig.getKpiId("id",  Constants.Kpi.NET_REVENUE, "a1").toString
        var payment: DataFrame = null
        var bpm = Try {
            payment = sqlContext.read.parquet(preInputPath + "/payment_2/" + logDate).select("game_code", "log_date","sid", "id", "net_amt")
        }
        val a = "a".toLowerCase()
        if (bpm.isSuccess) {
            Common.logger("Gross revenue start")
            payment.cache()
            val paymentT = payment.groupBy("game_code").agg(sum("net_amt")).orderBy("game_code")
            val paymentD = payment.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "game_code", "net_amt").groupBy("game_code", "f_log_date").agg(sum("net_amt")).orderBy("game_code", "f_log_date")
            reportByGroup(paymentD, paymentT, "kpi_paying", logDate, kpiId, sc)
            if (serverReport == "true") {
                val paymentServerT = payment.groupBy("sid").agg(sum("net_amt")).orderBy("sid")
                val paymentServerD = payment.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "sid", "net_amt").groupBy("sid", "f_log_date").agg(sum("net_amt")).orderBy("sid", "f_log_date")
                reportByGroupGroupByServer(gameCodeInput, paymentServerD, paymentServerT, "kpi_paying", logDate, kpiId, sc)
            }
            Common.logger("Gross revenue end")
            //15001
            Common.logger("Paying user start")
            kpiId = IdConfig.getKpiId("id", Constants.Kpi.PAYING_USER, "a1").toString
            reportByIncrement(payment, "kpi_paying", logDate, kpiId, sc)
            if (serverReport == "true") {
                reportByIncrementGroupByServer(gameCodeInput, payment, "kpi_paying", logDate, kpiId, sc)
            }
            Common.logger("Paying user end")
        }

        //10001
        Common.logger("Active user start")
        kpiId = IdConfig.getKpiId("id", Constants.Kpi.ACTIVE, "a1").toString
        var active: DataFrame = null
        var ba = Try {
            active = sqlContext.read.parquet(preInputPath + "/activity_2/" + logDate).select("game_code", "log_date", "sid", "id")
            //active = active.sort(active("id"), active("log_date")).dropDuplicates(Seq("id"))
        }
        if (ba.isSuccess) {
            active.cache()
            reportByIncrement(active, "kpi_user", logDate, kpiId, sc)
            if (serverReport == "true") {
                reportByIncrementGroupByServer(gameCodeInput, active, "kpi_user", logDate, kpiId, sc)
            }
            active.unpersist()
        }
        Common.logger("Active user end")

        //11001
        Common.logger("New register user start")
        kpiId = IdConfig.getKpiId("id", Constants.Kpi.ACCOUNT_REGISTER, "a1").toString
        var newRegister: DataFrame = null
        var bnr = Try {
            newRegister = sqlContext.read.parquet(preInputPath + "/accregister_2/" + logDate).select("game_code", "log_date", "sid", "id")
        }
        if (bnr.isSuccess) {
            newRegister.cache()
            val newRegisterT = newRegister.groupBy("game_code").agg(countDistinct("id")).orderBy("game_code")
            val newRegisterD = newRegister.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "game_code").groupBy("game_code", "f_log_date").agg(countDistinct("id")).orderBy("game_code", "f_log_date")
            reportByGroup(newRegisterD, newRegisterT, "kpi_accregister", logDate, kpiId, sc)
            if (serverReport == "true") {
                val newRegisterServerT = newRegister.groupBy("sid").agg(countDistinct("id")).orderBy("sid")
                val newRegisterServerD = newRegister.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "sid").groupBy("sid", "f_log_date").agg(countDistinct("id")).orderBy("sid", "f_log_date")
                reportByGroupGroupByServer(gameCodeInput, newRegisterServerD, newRegisterServerT, "kpi_accregister", logDate, kpiId, sc)
            }
            newRegister.unpersist()
        }
        Common.logger("New register user end")

        //19001
        Common.logger("New paying user start")
        kpiId = IdConfig.getKpiId("id", Constants.Kpi.NEW_PAYING, "a1").toString
        var newPaying: DataFrame = null
        var bnp = Try {
            newPaying = sqlContext.read.parquet(preInputPath + "/first_charge_2/" + logDate).select("game_code", "log_date", "sid", "id")
        }
        if (bnp.isSuccess) {
            newPaying.cache()
            val newPayingT = newPaying.groupBy("game_code").agg(countDistinct("id")).orderBy("game_code")
            val newPayingD = newPaying.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "game_code").groupBy("game_code", "f_log_date").agg(countDistinct("id")).orderBy("game_code", "f_log_date")
            reportByGroup(newPayingD, newPayingT, "kpi_first_charge", logDate, kpiId, sc)

            if (serverReport == "true") {
                val newPayingServerT = newPaying.groupBy("sid").agg(countDistinct("id")).orderBy("sid")
                val newPayingServerD = newPaying.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "sid").groupBy("sid", "f_log_date").agg(countDistinct("id")).orderBy("sid", "f_log_date")
                reportByGroupGroupByServer(gameCodeInput, newPayingServerD, newPayingServerT, "kpi_first_charge", logDate, kpiId, sc)
            }
        }
        Common.logger("New paying user end")

        if (payment != null && newPaying != null) {
            kpiId = IdConfig.getKpiId("id", Constants.Kpi.NEW_PAYING_NET_REVENUE, "a1").toString
            var npuGr1 = payment.as('p).join(newPaying.as('f), payment("game_code") === newPaying("game_code") && payment("id") === newPaying("id"), "leftsemi")
            val npuGr1T = npuGr1.groupBy("game_code").agg(sum("net_amt")).orderBy("game_code")
            val npuGr1D = npuGr1.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "game_code", "net_amt").groupBy("game_code", "f_log_date").agg(sum("net_amt")).orderBy("game_code", "f_log_date")
            reportByGroup(npuGr1D, npuGr1T, "kpi_first_charge", logDate, kpiId, sc)
            if (serverReport == "true") {
                val npuGr1ServerT = npuGr1.groupBy("sid").agg(sum("net_amt")).orderBy("sid")
                val npuGr1ServerD = npuGr1.selectExpr("date_format(log_date,\"yyyy-MM-dd HH\") as f_log_date", "id", "sid", "net_amt").groupBy("sid", "f_log_date").agg(sum("net_amt")).orderBy("sid", "f_log_date")
                reportByGroupGroupByServer(gameCodeInput, npuGr1ServerD, npuGr1ServerT, "kpi_first_charge", logDate, kpiId, sc)
            }
        }

        if (payment != null) {
            payment.unpersist()
        }
        if (newPaying != null) {
            newPaying.unpersist()
        }
        mysqlDB.close()
        sc.stop()
        Common.logger("Shutdown")
    }

    def reportByIncrement(dataDT: DataFrame, kpiType: String, logDate: String, kpiId: String, sc: SparkContext): Unit = {
        var dataJson: mutable.Map[String, mutable.Map[String, String]] = mutable.Map()
        for (i <- 0 to 23) {
            var datetimeLimit1 = ""
            var datetimeLimit2 = ""
            var datetimeStore = ""
            if (i < 9) {
                datetimeLimit1 = logDate + " 0" + (i + 1) + ":00:00"
            } else {
                datetimeLimit1 = logDate + " " + (i + 1) + ":00:00"
            }

            if (i < 10) {
                datetimeLimit2 = logDate + " 0" + i.toString + ":00:00"
                datetimeStore = "0" + i + ":00:00"
            } else {
                datetimeLimit2 = logDate + " " + i.toString + ":00:00"
                datetimeStore = i + ":00:00"
            }
            import sparkSesion.implicits._
            val whereCondition = "log_date < '" + datetimeLimit1 + "'"
            val dataSet:Dataset[(String, mutable.Map[String, String])] = dataDT.where(whereCondition).groupBy("game_code").agg(countDistinct("id")).coalesce(1).map { line: Row =>
                var gameCode = line(0).toString
                var number = line(1).toString
                var tmp: mutable.Map[String, String] = mutable.Map(datetimeStore -> number)
                (gameCode, tmp)
            }

            //var dtt: RDD[(String, mutable.Map[String, String])]  = dataSet.rdd
            //var dataJson_t: Map[String, mutable.Map[String, String]] = dtt.collect.toMap
            var dataJson_t: Map[String, mutable.Map[String, String]] = dataSet.rdd.collect.toMap

            dataJson_t.keys.foreach { gameCode =>
                var t: mutable.Map[String, String] = dataJson_t(gameCode)
                if (dataJson.contains(gameCode)) {
                    dataJson(gameCode) += (t.keys.toList(0) -> t.values.toList(0))
                } else {
                    dataJson += (gameCode -> t)
                }
            }
        }
        var dataMap: mutable.Map[String, String] = mutable.Map()
        var stt = 0
        dataJson.keys.foreach { gameCode =>
            var tmp: mutable.Map[String, String] = dataJson(gameCode)
            val valueString = getJsonString(tmp)
            val now = Calendar.getInstance().getTime()
            val currentDate = currentDateFormat.format(now)
            var maxValue = getMaxValue(tmp)
            insertIntoDB(logDate, gameCode, dataSource, kpiId, valueString, currentDate, tableHourly)
            insertIntoDB(logDate, gameCode, dataSource, kpiId, maxValue.toString, currentDate, tableDaily)
            val stringStore = logDate + "\t" + gameCode + "\t" + dataSource + "\t" + kpiId + "\t" + valueString + "\t" + currentDate
            dataMap += (stt.toString -> stringStore)
            stt = stt + 1
        }
        storeFile(dataMap, outputPath + "/" + kpiType + "/" + logDate, sc)

    }

    def reportByIncrementGroupByServer(gameCode: String, dataDT: DataFrame, kpiType: String, logDate: String, kpiId: String, sc: SparkContext): Unit = {
        var dataJson: mutable.Map[String, mutable.Map[String, String]] = mutable.Map()
        for (i <- 0 to 23) {
            var datetimeLimit1 = ""
            var datetimeLimit2 = ""
            var datetimeStore = ""
            if (i < 9) {
                datetimeLimit1 = logDate + " 0" + (i + 1) + ":00:00"
            } else {
                datetimeLimit1 = logDate + " " + (i + 1) + ":00:00"
            }

            if (i < 10) {
                datetimeLimit2 = logDate + " 0" + i.toString + ":00:00"
                datetimeStore = "0" + i + ":00:00"
            } else {
                datetimeLimit2 = logDate + " " + i.toString + ":00:00"
                datetimeStore = i + ":00:00"
            }
            import sparkSesion.implicits._
            val whereCondition = "log_date < '" + datetimeLimit1 + "'"
            val dataSet:Dataset[(String, mutable.Map[String, String])] = dataDT.where(whereCondition).groupBy("sid").agg(countDistinct("id")).coalesce(1).map { line: Row =>
                var serverId = line(0).toString
                var number = line(1).toString
                var tmp: mutable.Map[String, String] = mutable.Map(datetimeStore -> number)
                (serverId, tmp)
            }
            //var dtt: RDD[(String, mutable.Map[String, String])] = dataSet.rdd
            var dataJson_t: Map[String, mutable.Map[String, String]] = dataSet.rdd.collect.toMap

            dataJson_t.keys.foreach { serverId =>
                var t: mutable.Map[String, String] = dataJson_t(serverId)
                if (dataJson.contains(serverId)) {
                    dataJson(serverId) += (t.keys.toList(0) -> t.values.toList(0))
                } else {
                    dataJson += (serverId -> t)
                }
            }
        }
        var dataMap: mutable.Map[String, String] = mutable.Map()
        var stt = 0
        dataJson.keys.foreach { serverId =>
            var tmp: mutable.Map[String, String] = dataJson(serverId)
            val valueString = getJsonString(tmp)
            val now = Calendar.getInstance().getTime()
            val currentDate = currentDateFormat.format(now)
            var maxValue = getMaxValue(tmp)
            insertIntoDB(logDate, gameCode, serverId, dataSource, kpiId, valueString, currentDate, tableServerHourly)
            //insertIntoDB(logDate, gameCode, serverId, dataSource, kpiId, maxValue.toString, currentDate, tableDaily)
            val stringStore = logDate + "\t" + gameCode + "\t" + serverId + "\t" + dataSource + "\t" + kpiId + "\t" + valueString + "\t" + currentDate
            dataMap += (stt.toString -> stringStore)
            stt = stt + 1
        }

    }

    def reportByGroup(dataDT: DataFrame, dataDTTotal: DataFrame, kpiType: String, logDate: String, kpiId: String, sc: SparkContext): Unit = {
        if (dataDT == null || dataDTTotal == null) {
            return
        }
        //total first
        var dtt: RDD[(String, String)] = dataDTTotal.rdd.coalesce(1).map { line =>
            var gameCode = line(0).toString
            var totalNumber = line(1).toString
            (gameCode, totalNumber)
        }
        var dataTotal: Map[String, String] = dtt.collect.toMap
        var dataJson: mutable.Map[String, mutable.Map[String, String]] = mutable.Map()
        var dataMap: mutable.Map[String, String] = mutable.Map()

        dataDT.coalesce(1).collect.foreach { line =>
            val gameCode: String = line(0).toString
            val dateTime: String = line(1).toString
            var number = line(2)
            if(number == null){
                number = 0
            }
            var t: mutable.Map[String, String] = mutable.Map()
            t += (dateTime.split(" ")(1) + ":00:00" -> number.toString)

            if (dataJson.contains(gameCode)) {
                dataJson(gameCode) += (dateTime.split(" ")(1) + ":00:00" -> number.toString)
            } else {
                dataJson += (gameCode -> t)
            }
        }
        var stt = 0
        dataJson.keys.foreach { gameCode =>
            var t: mutable.Map[String, String] = dataJson(gameCode)
            t += ("total" -> dataTotal(gameCode))
            val valueString = getJsonString(t)
            val now = Calendar.getInstance().getTime()
            val currentDate = currentDateFormat.format(now)
            insertIntoDB(logDate, gameCode, dataSource, kpiId, valueString, currentDate, tableHourly)
            insertIntoDB(logDate, gameCode, dataSource, kpiId, dataTotal(gameCode).toString, currentDate, tableDaily)
            val stringStore = logDate + "\t" + gameCode + "\t" + dataSource + "\t" + kpiId + "\t" + valueString + "\t" + currentDate
            dataMap += (stt.toString -> stringStore)
            stt = stt + 1

        }
        storeFile(dataMap, outputPath + "/" + kpiType + "/" + logDate, sc)
    }

    def reportByGroupGroupByServer(gameCode: String, dataDT: DataFrame, dataDTTotal: DataFrame, kpiType: String, logDate: String, kpiId: String, sc: SparkContext): Unit = {
        if (dataDT == null || dataDTTotal == null || dataDT.count == 0 || dataDTTotal.count == 0) {
            return
        }
        //total first
        var dtt: RDD[(String, String)] = dataDTTotal.rdd.coalesce(1).map { line =>
            var serverId = line(0).toString
            var totalNumber = line(1).toString
            (serverId, totalNumber)
        }
        var dataTotal: Map[String, String] = dtt.collect.toMap
        var dataJson: mutable.Map[String, mutable.Map[String, String]] = mutable.Map()
        var dataMap: mutable.Map[String, String] = mutable.Map()

        dataDT.coalesce(1).collect.foreach { line =>
            val serverId: String = line(0).toString
            val dateTime: String = line(1).toString
            var number = line(2)
            if(number == null){
                number = 0
            }
            var t: mutable.Map[String, String] = mutable.Map()
            t += (dateTime.split(" ")(1) + ":00:00" -> number.toString)

            if (dataJson.contains(serverId)) {
                dataJson(serverId) += (dateTime.split(" ")(1) + ":00:00" -> number.toString)
            } else {
                dataJson += (serverId -> t)
            }
        }
        var stt = 0
        dataJson.keys.foreach { serverId =>
            var t: mutable.Map[String, String] = dataJson(serverId)
            t += ("total" -> dataTotal(serverId))
            val valueString = getJsonString(t)
            val now = Calendar.getInstance().getTime()
            val currentDate = currentDateFormat.format(now)
            insertIntoDB(logDate, gameCode, serverId, dataSource, kpiId, valueString, currentDate, tableServerHourly)
            //insertIntoDB(logDate, gameCode, serverId, dataSource, kpiId, dataTotal(serverId).toString, currentDate, tableDaily)
            val stringStore = logDate + "\t" + gameCode + "\t" + serverId + "\t" + dataSource + "\t" + kpiId + "\t" + valueString + "\t" + currentDate
            dataMap += (stt.toString -> stringStore)
            stt = stt + 1

        }
        storeFile(dataMap, outputPath + "/" + kpiType + "/" + logDate, sc)
    }

    def storeFile(dataMap: mutable.Map[String, String], path: String, sc: SparkContext): Unit = {
        /*
        val array_size = dataMap.keys.size
        if (array_size != 0) {
            var stt = 0
            val data_arr = new Array[String](array_size)
            dataMap.keys.foreach { key =>
                val data_a = dataMap(key)
                data_arr(stt) = data_a
                stt = stt + 1
            }
            sc.parallelize(data_arr).saveAsTextFile(path)
        }
        */
    }

    def insertIntoDB(logDate: String, gameCode: String, dataSource: String, kpiId: String, valueString: String, current_date: String, tableName: String): Unit = {
        val sqlDelete = "delete from " + tableName + " where game_code = '" + gameCode + "'" +
            " and source = '" + dataSource + "'" +
            " and report_date = '" + logDate + "'" +
            " and kpi_id = " + kpiId
        //mysqlDB.executeUpdate(sqlDelete, false)
        mysql.executeUpdate(sqlDelete,null)
        val sql = "insert into " + tableName + " (report_date, game_code, source, kpi_id, kpi_value, calc_date) values(" +
            "'" + logDate + "','" + gameCode + "','" + dataSource + "'," + kpiId + ",'" + valueString + "','" + current_date + "')"
        //mysqlDB.executeUpdate(sql, false)
        mysql.executeUpdate(sql,null)
    }

    def insertIntoDB(logDate: String, gameCode: String, serverId: String, dataSource: String, kpiId: String, valueString: String, current_date: String, tableName: String): Unit = {
        val sqlDelete = "delete from " + tableName + " where game_code = '" + gameCode + "'" +
            " and server_id = '" + serverId + "'" +
            " and source = '" + dataSource + "'" +
            " and report_date = '" + logDate + "'" +
            " and kpi_id = " + kpiId
        //mysqlDB.executeUpdate(sqlDelete, false)
        mysql.executeUpdate(sqlDelete, null)
        val sql = "insert into " + tableName + " (report_date, game_code, server_id, source, kpi_id, kpi_value, calc_date) values(" +
            "'" + logDate + "','" + gameCode + "','" + serverId + "','" + dataSource + "'," + kpiId + ",'" + valueString + "','" + current_date + "')"
        //mysqlDB.executeUpdate(sql, false)
        mysql.executeUpdate(sql, null)
    }

    def getJsonString(data: mutable.Map[String, String]): String = {
        var data_string_1 = "{"
        data.foreach { line =>
            val (key, value) = line
            if (key.toString != "" && value.toString != "") {
                data_string_1 += "\"" + key + "\"" + ":" + "\"" + value + "\""
                data_string_1 += ","
            }
        }
        if (data_string_1 != "{") {
            val data_string_2 = data_string_1.dropRight(1) + "}"
            return data_string_2
        }
        return NO_DATA
    }

    def getMaxValue(data: mutable.Map[String, String]): Long = {
        var vReturn = 0L
        data.foreach { line =>
            val (key, value) = line
            if (key.toString != "" && value.toString != "") {
                if (vReturn < value.toLong) {
                    vReturn = value.toLong
                }
            }
        }
        vReturn
    }
}