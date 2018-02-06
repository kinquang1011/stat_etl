package vng.ge.stats.etl.transform.adapter.myplay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.MyplayFormatter
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils}

/**
  * Created by quangctn on 09/02/2017.
  */
class Myplay_Bacay extends MyplayFormatter("myplay_bacay") {

    import sqlContext.implicits._

    def start(args: Array[String]): Unit = {
        initParameters(args)
        this -> run -> close
    }
    override def getActivityDs(logDate: String, hourly: String): DataFrame = {
        var logRaw: RDD[String] = null
        if (hourly == "") {
            val logPattern = Constants.WAREHOUSE_DIR + "/myplay_bacay/user/[yyyy-MM-dd]/*"
            val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
            logRaw = getRawLog(logPath)
        } else {
            //hourly
        }
        val filterLog = (line: String) => {
            var rs = false
            if (line.startsWith(logDate)) {
                rs = true
            }
            rs
        }
        val sf = Constants.FIELD_NAME
        val loginLogoutDs = logRaw.map(line => line.split("\\t")).filter(line => filterLog(line(0))).map { line =>
            val dateTime = line(0)
            val id = line(1)
            val action = "login"
            ("myplay_bacay", dateTime, action, id)
        }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID)
        loginLogoutDs
    }

    override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
        var paymentRaw: RDD[String] = null
        if (hourly == "") {
            val paymentPatternPath = Constants.GAMELOG_DIR + "/myplay_payment_db/[yyyyMMdd]/Cash_BaCay*"
            val paymentPath = PathUtils.generateLogPathDaily(paymentPatternPath, logDate)
            paymentRaw = getRawLog(paymentPath)
        }
        val filterLog = (line: String) => {
            var rs = false
            if (line.startsWith(logDate)) {
                rs = true
            }
            rs
        }
        val sf = Constants.FIELD_NAME
        val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filterLog(line(6))).map { line =>
            val dateTime = line(6)
            val money = line(4)
            val id = line(2)
            ("myplay_bacay", dateTime, id, money, money)
        }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT)
        paymentDs
    }

}
