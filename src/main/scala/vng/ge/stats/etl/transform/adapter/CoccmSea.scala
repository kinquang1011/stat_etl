package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.Formatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, PathUtils}

/**
 * Created by tuonglv on 26/12/2016.
 */
class CoccmSea extends Formatter ("coccmsea") {

    import sqlContext.implicits._

    def start(args: Array[String]): Unit = {
        initParameters(args)
        //this->run->close()
        this->otherETL(_logDate, _hourly, "")
    }
    
    override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
        getTownHallLevelDs(logDate, hourly)
    }

    override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
        var paymentRaw: RDD[String] = null
        if (hourly == "") {
            //val paymentPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/thoiloanm/thoiloanm_[yyyyMMdd].gz"
            val paymentPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/thoiloanm/thoiloanm_*"
            val paymentPath = PathUtils.generateLogPathDaily(paymentPattern, logDate)
            paymentRaw = getRawLog(paymentPath)
        }
        val sc = Constants.FIELD_NAME
        val filter = (s1:String, s2:String) => {
            var rs = false
            if(s1.toInt > 0 && MyUdf.timestampToDate(s2.toLong).startsWith(logDate))
                rs = true
            rs
        }
        val paymentDs = paymentRaw.map(line => line.split("\\|")).filter(r => filter(r(5),r(3))).map { r =>
            val logDate = MyUdf.timestampToDate(r(3).toLong)
            ("coccmsea", logDate, r(0), r(4), r(10), r(10), r(8), r(15))
        }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.SID, sc.GROSS_AMT, sc.NET_AMT, sc.ACTION, sc.TRANS_ID)
        paymentDs
    }

    override def getActivityDs(logDate: String, hourly: String): DataFrame = {
        var logoutRaw: RDD[String] = null
        var loginRaw: RDD[String] = null
        if (hourly == "") {
            //val loginPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/tlm_Login/tlm_Login-[yyyy-MM-dd].gz"
            val loginPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/tlm_Login/tlm_Login*"
            val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
            loginRaw = getRawLog(loginPath)
            //val logoutPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/tlm_Logout/tlm_Logout-[yyyy-MM-dd].gz"
            val logoutPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/tlm_Logout/tlm_Logout*"
            val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
            logoutRaw = getRawLog(logoutPath)
        } else {
            //hourly
        }
        val filter = (s1:String) => {
            var rs = false
            if(MyUdf.timestampToDate(s1.toLong * 1000).startsWith(logDate))
                rs = true
            rs
        }

        val sc = Constants.FIELD_NAME
        val loginDS = loginRaw.map(line => line.split("\\|")).filter(line => filter(line(0))).map { r =>
            val logDate = MyUdf.timestampToDate(r(0).toLong * 1000)
            val channel = MyUdf.rawLoginChanelToChannel(r(32))
            ("coccmsea", logDate, r(3), r(12), channel, "login")
        }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.LEVEL, sc.CHANNEL, sc.ACTION)

        val logoutDS = logoutRaw.map(line => line.split("\\|")).filter(line => filter(line(0))).map { r =>
            val logDate = MyUdf.timestampToDate(r(0).toLong * 1000)
            val channel = MyUdf.rawLoginChanelToChannel(r(32))
            ("coccmsea", logDate, r(3), r(12), channel, "logout")
        }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, sc.LEVEL, sc.CHANNEL, sc.ACTION)

        val ds: DataFrame = loginDS.unionAll(logoutDS)
        ds
    }
    
    def getTownHallLevelDs(logDate: String, hourly: String) = {
        
        val gameCode = this.gameCode
        var logoutRaw: RDD[String] = null
        var loginRaw: RDD[String] = null
        
        if (hourly == "") {
    
            val loginPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/tlm_Login/tlm_Login*"
            val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
            loginRaw = getRawLog(loginPath)
            
            val logoutPattern = Constants.GAME_LOG_DIR + "/coccmsea/[yyyy-MM-dd]/tlm_Logout/tlm_Logout*"
            val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
            logoutRaw = getRawLog(logoutPath)
        }
    
        val filter = (s1:String) => {
            var rs = false
            if(MyUdf.timestampToDate(s1.toLong * 1000).startsWith(logDate))
                rs = true
            rs
        }
    
        val sc = Constants.FIELD_NAME
        val loginDF = loginRaw.map(line => line.split("\\|")).filter(line => filter(line(0))).map { r =>
            val logDate = MyUdf.timestampToDate(r(0).toLong * 1000)
            val channel = MyUdf.rawLoginChanelToChannel(r(32))
            ("coccmsea", logDate, r(3), r(23))
        }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, "group")
    
        val logoutDF = logoutRaw.map(line => line.split("\\|")).filter(line => filter(line(0))).map { r =>
            val logDate = MyUdf.timestampToDate(r(0).toLong * 1000)
            val channel = MyUdf.rawLoginChanelToChannel(r(32))
            ("coccmsea", logDate, r(3), r(23))
        }.toDF(sc.GAME_CODE, sc.LOG_DATE, sc.ID, "group")
        
        val logDF = loginDF.union(logoutDF)
        val result = logDF.orderBy(desc("id"), desc("log_date")).dropDuplicates("id")
        
        val path = PathUtils.getParquetPath(gameCode, logDate, Constants.WAREHOUSE_DIR, "data", "hall_level")
        storeInParquet(result, path)
    }
}