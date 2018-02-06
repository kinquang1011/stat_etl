package vng.ge.stats.test

/**
  * Created by canhtq on 30/03/2017.
  */

import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{DateTimeUtils}
import vng.ge.stats.report.util.PathUtils

import scala.util.matching.Regex
object RegexTest {
  def main(args: Array[String]) {

    val datetime = "2017-04-26 00:01:21"
    var min = datetime.substring(17, 19).toInt
    print(min)
    val str = DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:00", datetime)
    println(str)
    val code = MyUdf.getSdkLoginGameId("hdfs://c408.hadoop.gda.lo:8020/ge/gamelogs/sdk/2017-05-03/CFMOBILE_Login_InfoLog-2017-05-03.gz")
    println(code)

    val logPattern = Constants.GAME_LOG_DIR + "/sdk/[yyyy-MM-dd]/" + "nkd"  + "_Login_InfoLog/"
    val logDate="2016-02-30"
    //val paths = PathUtils.generateLogPathDaily(logPattern,logDate)

    val paths = PathUtils.getFilePatterns("ahahha", logDate, "a7")
    println(paths)
    println(MyUdf.timestampToDate(("1498618761".toLong) * 1000))
  }
}
