package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.Formatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
 * Created by tuonglv on 16/01/2017.
 */
class NikkiCn extends Formatter("nikkicn") {

    val timeIncr: Long = -3600
    val convertRate = 22500

    import sqlContext.implicits._

    def start(args: Array[String]): Unit = {
        initParameters(args)
        setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)
        this -> run -> close
    }

    override def getActivityDs(logDate: String, hourly: String): DataFrame = {
        val _timeIncr = timeIncr
        var loginLogoutRaw: RDD[String] = null
        if (hourly == "") {
            val logPattern = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/loggame/ntlog.log-[yyyyMMdd].gz"
            var logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
            val nextDayFromLogDate = DateTimeUtils.getDateDifferent(1, logDate, Constants.TIMING, Constants.A1)
            ///ge/gamelogs/nikkicn/20170213/loggame/ntlog.log-2017021301
            val getOneMoreHour = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/loggame/ntlog.log*"
            val logPathOneMoreHour = PathUtils.generateLogPathDaily(getOneMoreHour, nextDayFromLogDate, numberOfDay = 1)
            logPath = logPath ++ logPathOneMoreHour
            loginLogoutRaw = getRawLog(logPath)
        } else {
            val logPattern = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/loggame/ntlog.*"
            val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
            loginLogoutRaw = getRawLog(logPath)
        }

        val getOs = (s: String) => {
            var rs = "other"
            if (s == "0") {
                rs = "ios"
            } else if (s == "1") {
                rs = "android"
            }
            rs
        }
        val filterLoginLogout = (line: Array[String]) => {
            var rs = false
            if (line.length >= 25 && MyUdf.dateTimeIncrement(line(2), _timeIncr).startsWith(logDate) && (line(0) == "PlayerLogin" || line(0) == "PlayerLogout")) {
                rs = true
            }
            rs
        }
        val sf = Constants.FIELD_NAME
        val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).filter(line => filterLoginLogout(line)).map { line =>
            val id = line(6)
            val serverId = line(1)
            val dateTime = MyUdf.dateTimeIncrement(line(2), _timeIncr)
            val os = getOs(line(4))

            var level = ""
            var did = ""
            var carrier = ""
            var network = ""
            var scrW = ""
            var scrH = ""
            var loginChannel = ""
            var device = ""
            var onlineTime = "0"

            var action = ""
            if (line(0) == "PlayerLogin") {
                action = "login"
                level = line(7)
                did = line(22)
                carrier = line(12)
                network = line(13)
                scrW = line(14)
                scrH = line(15)
                loginChannel = line(17)
                device = line(11)
            } else {
                action = "logout"
                level = line(8)
                did = line(23)
                carrier = line(13)
                network = line(14)
                scrW = line(15)
                scrH = line(16)
                loginChannel = line(18)
                device = line(12)
                onlineTime = line(7)
            }
            val resolution = scrW + "," + scrH
            ("nikkicn", dateTime, serverId, action, id, level, did, os, device, loginChannel, onlineTime, carrier, network, resolution)
        }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ACTION, sf.ID, sf.LEVEL, sf.DID, sf.OS, sf.DEVICE, sf.CHANNEL, sf.ONLINE_TIME, sf.CARRIER, sf.NETWORK, sf.RESOLUTION)
        loginLogoutDs
    }

    //Nikki_Logcharge_2017-02-07.csv.gz
    override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
        val _timeIncr = timeIncr
        var raw: RDD[String] = null
        if (hourly == "") {
            val logPattern = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/log_recharge/Nikki_Logcharge_[yyyy-MM-dd].csv.gz"
            var logPath = PathUtils.generateLogPathDaily(logPattern, logDate)

            val nextDayFromLogDate = DateTimeUtils.getDateDifferent(1, logDate, Constants.TIMING, Constants.A1)
            val currentDate = DateTimeUtils.getCurrentDate()

            var logPathOneMoreHour = Array[String]()
            if(nextDayFromLogDate == currentDate) {
                // neu ngay xu ly log la yesterday, tuc la report daily
                ///ge/gamelogs/nikkicn/20170213/chargelog_hourly/Nikki_Logcharge_2017-02-13-01.csv
                val getOneMoreHour = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/chargelog_hourly/Nikki_Logcharge_[yyyy-MM-dd]-01.csv"
                logPathOneMoreHour = PathUtils.generateLogPathDaily(getOneMoreHour, nextDayFromLogDate, numberOfDay = 1)
            }else {
                // truong hop chay lai
                val getOneMoreHour = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/log_recharge/Nikki_Logcharge_[yyyy-MM-dd].csv.gz"
                logPathOneMoreHour = PathUtils.generateLogPathDaily(getOneMoreHour, nextDayFromLogDate, numberOfDay = 1)
            }
            logPath = logPath ++ logPathOneMoreHour
            raw = getRawLog(logPath)
        }else{
            val logPattern = Constants.GAME_LOG_DIR + "/nikkicn/[yyyyMMdd]/chargelog_minly/Nikki_Logcharge_*.csv"
            val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
            raw = getRawLog(logPath)
        }

        val trimQuote = (s: String) => {
            s.replace("\"", "")
        }
        val firstFilter = (line: Array[String]) => {
            var rs = false
            if (line.length >= 17 && MyUdf.dateTimeIncrement(line(11), _timeIncr).startsWith(logDate)) {
                rs = true
            }
            rs
        }
        val getOs = (s: String) => {
            var rs = "other"
            if (s == "0") {
                rs = "ios"
            } else if (s == "1") {
                rs = "android"
            }
            rs
        }

        val convertMap = Map(
            "com.pg2.nikkicn.diamond120" -> 1.99,
            "com.pg2.nikkicn.diamond188" -> 2.99,
            "com.pg2.nikkicn.diamond377" -> 5.99,
            "com.pg2.nikkicn.diamond668" -> 9.99,
            "com.pg2.nikkicn.diamond1368" -> 19.99,
            "com.pg2.nikkicn.diamond4288" -> 59.99,
            "com.pg2.nikkicn.diamond7288" -> 99.99,
            "com.pg2.nikkicn.gift" -> 0.99
        )
        val _convertRate = convertRate

        val getGross = (s: String) => {
            val ss = s.split("-")
            var vnd: Double = 0
            if (ss.length >= 5) {
                val itemid = ss(4)
                var usd: Double = 0
                if (convertMap.contains(itemid)) {
                    usd = convertMap(itemid)
                }
                vnd = usd * _convertRate
            }
            vnd
        }

        val sf = Constants.FIELD_NAME
        val paymentDs = raw.map(line => line.split("\",\"")).filter(line => firstFilter(line)).map { line =>
            val trans = trimQuote(line(2))
            //val id = trimQuote(line(16)) //<-- openId
            val id = trimQuote(line(3))
            val money = line(5)
            val os = getOs(line(15))
            val dateTime = MyUdf.dateTimeIncrement(line(11), _timeIncr)
            val netRev = money
            val grossRev = getGross(trimQuote(line(0)))
            ("nikkicn", dateTime, id, os, netRev, grossRev, trans)
        }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.OS, sf.NET_AMT, sf.GROSS_AMT, sf.TRANS_ID)
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
            if (line.length == 3 && line(0).toLowerCase.startsWith("nikkitq")) {
                if (line(0).split("_").length == 2) {
                    rs = true
                }
            }
            rs
        }

        val sf = Constants.FIELD_NAME
        val ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
            val t = line(0).split("_")
            val serverId = t(1)
            val ccu = line(1)
            val timeStamp = line(2).toLong
            val dateTime = MyUdf.timestampToDate(timeStamp)
            ("nikkicn", serverId, ccu, dateTime)
        }.toDF(sf.GAME_CODE, sf.SID, sf.CCU, sf.LOG_DATE)
        ccuDs
    }
}