package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.Formatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
 * Created by tuonglv on 09/01/2017.
 */
class Jxm extends Formatter("jxm") {

    import sqlContext.implicits._

    def start(args: Array[String]): Unit = {
        initParameters(args)
        this -> run -> close
    }

    override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
        var paymentRaw: RDD[String] = null
        if (hourly == "") {
            val paymentPattern = Constants.GAME_LOG_DIR + "/jxm/[yyyyMMdd]/datalog/real/*/tlog/[yyyyMMdd].log.gz"
            val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
            paymentRaw = getRawLog(paymentPath)
        }
        val getOs = (s: String) => {
            var platform = "other"
            if (s == "0") {
                platform = "ios"
            } else if (s == "1") {
                platform = "android"
            }
            platform
        }
        val paymentFilter = (line: Array[String]) => {
            var rs = false
            if (line(0) == "RechargeFlow" && line.length > 10 && line(2).startsWith(logDate)) {
                rs = true
            }
            rs
        }

        val sc = Constants.FIELD_NAME
        val paymentDs = paymentRaw.map(line => line.split("\\|")).filter(line => paymentFilter(line)).map { line =>
            val platform = getOs(line(4))
            val netRev = line(9).toDouble * 100
            val grossRev = netRev
            val dateTime = line(2)
            val sid = line(5)
            val rid = line(7)
            val id = line(6)
            val ip = line(8)
            ("jxm", dateTime, id, rid, sid, platform, netRev, grossRev, ip)
        }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.RID, sc.SID, sc.OS, sc.NET_AMT, sc.GROSS_AMT, sc.IP)
        paymentDs
    }

    override def getActivityDs(logDate: String, hourly:String): DataFrame = {
        var loginLogoutRaw: RDD[String] = null
        if (hourly == "") {
            val logPattern = Constants.GAME_LOG_DIR + "/jxm/[yyyyMMdd]/datalog/real/*/tlog/[yyyyMMdd].log.gz"
            val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
            loginLogoutRaw = getRawLog(logPath)
        }

        val getOs = (s: String) => {
            var platform = "other"
            if (s == "0") {
                platform = "ios"
            } else if (s == "1") {
                platform = "android"
            }
            platform
        }

        val filterLoginLogout = (line:Array[String]) => {
            var rs = false
            if ((line(0) == "PlayerLogin" || line(0) == "PlayerLogout") && line.length > 25 && line(2).startsWith(logDate)) {
                rs = true
            }
            rs
        }
        val sf = Constants.FIELD_NAME
        val loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).filter(line => filterLoginLogout(line)).map { line =>
            val platform = getOs(line(4))
            val dateTime = line(2)
            val sid = line(5)
            val rid = line(7)
            val id = line(6)

            var online_time = ""
            var level = ""
            var did = ""
            var device = ""
            var action = ""
            if (line(0) == "PlayerLogin") {
                online_time = "0"
                level = line(8)
                did = line(23)
                device = line(12)
                action = "login"
            } else {
                online_time = line(8)
                level = line(9)
                did = line(24)
                device = line(13)
                action = "logout"
            }
            ("jxm", dateTime, action, id, rid, sid, online_time, level, platform, did, device)
        }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.ONLINE_TIME, sf.LEVEL, sf.OS, sf.DID, sf.DEVICE)
        loginLogoutDs
    }
}