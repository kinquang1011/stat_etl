package vng.ge.stats.etl.transform.udf

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.functions.udf
import vng.ge.stats.etl.utils.DateTimeUtils

import scala.util.Try
import scala.util.matching.Regex

/**
 * Created by tuonglv on 27/12/2016.
 */
object MyUdf {
    /**
      * input: 1482255690000
      * output: 2016-12-21 00:41:30
      */
    val timestampToDate = (s: Long) => {
        DateTimeUtils.getDate(s)
    }

    /**
      * input: 2016-12-21 00:41:30
      * output: 1482255690000
      */
    val dateToTimestamp = (dateTime: String) => {
        val timeStamp = DateTimeUtils.getTimestamp(dateTime)
        timeStamp
    }

    /**
      * input: fb.1312643535341
      * output: fb
      */
    val rawLoginChanelToChannel = (s: String) => {
        val t1 = s.split("\\.")
        var rs = ""
        if (t1.length == 2) {
            rs = t1(0)
        }
        rs
    }

    /**
      * input: 2016-12-21 00:41:30, 3600
      * output: 2016-12-21 01:41:30
      */
    var dateTimeIncrement = (dateTime: String, inc: Long) => {
        var newDateTime = ""
        Try {
            val timeStamp = DateTimeUtils.getTimestamp(dateTime)
            newDateTime = DateTimeUtils.getDate(timeStamp + inc * 1000)
        }
        newDateTime
    }
    /**
      * canhtq
      * path file format: hdfs://c408.hadoop.gda.lo:8020/ge/gamelogs/nh/20170329/2/login.txt ==> sid=2
      */
    var getSid = (path: String, date: String) => {
        var sid = "0"
        Try {
            val pattern = new Regex("([0-9]{4}[0-9]{2}[0-9]{2}\\/[0-9]{1,200})")
            val result = (pattern findAllIn path).mkString(",")
            if (!result.equalsIgnoreCase("")) {
                sid = result.replaceAll(date + "/", "")
            }

        }
        sid
    }
    val roundCcuDummy = udf { (timeStamps: String) => {
        val newTime = timeStamps.substring(0, timeStamps.length - 1) + "0"
        DateTimeUtils.getDate(newTime.toLong * 1000)
    }
    }
    val roundCCUTime = udf { (datetime: String) => {
        var min = datetime.substring(14, 16).toInt
        min = min - (min % 5)
        DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH", datetime) + ":%02d:00".format(min)
    }
    }
    val ccuTimeDropSeconds = udf { (datetime: String) => {
        DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:00", datetime)
    }
    }

    /**
      * canhtq
      * path file format: hdfs://c408.hadoop.gda.lo:8020/ge/gamelogs/sdk/2017-05-03/CFMOBILE_Login_InfoLog-2017-05-03.gz ==> CFMOBILE
      */
    var getSdkLoginGameId = (path: String) => {
        var gameid = "0"
        Try {
            val lpath = path.toLowerCase()
            val pattern = new Regex("(\\/[a-z0-9]{0,100}_login_infolog\\/)")
            val result = (pattern findAllIn lpath).mkString(",")
            if (!result.equalsIgnoreCase("")) {
                gameid = result.replaceAll("/", "")
                gameid = gameid.replaceAll("_login_infolog", "")
            }

        }
        gameid.toLowerCase
    }
    var getPaymentName = (path: String, date: String) => {
        var gameName = ""
        Try {
            //
            val pattern = new Regex("([0-9]{8}/Cash_[a-zA-Z]{1,100})")
             val result = (pattern findAllIn path).mkString(",")
            if (!result.equalsIgnoreCase("")) {
                gameName = result.replaceAll(date+ "/", "")
            }
        }
        gameName
    }

    val udfSdkLoginGameId = udf { (path: String) => {
        getSdkLoginGameId(path)
    }
    }

    val changeReportTime = udf { (datetime: String, logTimeZone: String) => {
        var gmt = 7
        gmt = gmt - logTimeZone.toInt
        val time = dateTimeIncrement(datetime, gmt * 3600)
        time
    }
    }
}
