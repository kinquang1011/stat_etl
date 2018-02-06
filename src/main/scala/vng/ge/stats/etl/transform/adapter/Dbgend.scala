package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.Common

/**
  * Created by quangctn on 28/11/2017.
  */
class Dbgend extends FairyFormatter("dbgend") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    var logDate: String = ""
    for (x <- args) {
      val xx = x.split("=")
      if (xx(0).contains("logDate")) {
        logDate = xx(1)
      }
    }

    this -> otherETL(logDate, "", "") -> close
  }


  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
    var _logDate = logDate
    if (logDate < "2017-11-01") {
      _logDate = logDate.substring(0, 7)
    }
    Common.logger("date :"+_logDate)
    val df = getDataDbg(_logDate)
    val outputPath = Constants.FAIRY_WAREHOUSE_DIR + s"/dbg/dbgend/${_logDate}/"
    df.coalesce(1).write.mode("overwrite").format("parquet").save(outputPath)

  }

  def getDataDbg(logDate: String): DataFrame = {
    var dbgDf = emptyDataFrame
    val filterSucc = (line: Array[String]) => {
      var rs = false
      if (line.length > 33) {
        rs = line(26).equalsIgnoreCase("SUCCESSFUL")
      }
      rs
    }
    var path = ""
    if (logDate.length == 7) {
      path = Constants.GAMELOG_DIR + s"/dbg/$logDate*/dbgend*"
    } else {
      path = Constants.GAMELOG_DIR + s"/dbg/$logDate/dbgend*"
    }
    val raw = getRawLog(Array(path))
    dbgDf = raw.map(line => line.split("\\t")).filter(line => filterSucc(line)).map { r =>
      val transid = r(0)
      val appid = r(1)
      val userid = r(2)
      val platform = r(3)
      val flow = r(4)
      val serverid = r(5)
      val reqdate = r(6)
      /* val length = r.length*/
      val itemid = r(7)
      val itemname = r(8)
      val itemquantity = r(9)
      val chargeamt = r(10)
      val feclientid = r(11)
      val env = r(12)
      val pmcid = r(13)
      val pmctransid = r(14)
      val pmcgrosschargeamt = r(15)
      val pmcnetchargeamt = r(16)
      val pmcchargeresult = r(17)
      val pmcupddate = r(18)
      val appnotifyresult = r(19)
      val appupddate = r(20)
      val isretrytonotify = r(21)
      val noretrytonotify = r(22)
      val lastretrytonotify = r(23)
      val hostname = r(24)
      val hostdate = r(25)
      val transstatus = r(26)
      val step = r(27)
      val stepresult = r(28)
      val isprocess = r(29)
      val transphase = r(30)
      val exceptions = r(31)
      val apptransid = r(32)
      val netchargeamt = r(33)
      /*  val addinfo = r(34)
        val YMD = r(35)
        val FileName = r(36)*/
      var otherInfo: Map[String, String] = Map()
      otherInfo = Map(
        "flow" -> flow,
        "itemquantity" -> itemquantity,
        "chargeamt" -> chargeamt,
        "feclientid" -> feclientid,
        "env" -> env,
        "pmctransid" -> pmctransid,
        "pmcchargeresult" -> pmcchargeresult,
        "pmcupddate" -> pmcupddate,
        "appnotifyresult" -> appnotifyresult,
        "appupddate" -> appupddate,
        "isretrytonotify" -> isretrytonotify,
        "noretrytonotify" -> noretrytonotify,
        "lastretrytonotify" -> lastretrytonotify,
        "hostname" -> hostname,
        "hostdate" -> hostdate,
        "transstatus" -> transstatus,
        "step" -> step,
        "stepresult" -> stepresult,
        "isprocess" -> isprocess,
        "transphase" -> transphase,
        "exceptions" -> exceptions,
        "apptransid" -> apptransid
      )
      (transid, appid, userid, platform, serverid, pmcid, pmcgrosschargeamt, pmcnetchargeamt, itemid, itemname, netchargeamt, reqdate, otherInfo)
    }.toDF("transid", "appid", "userid", "platform", "serverid", "pmcid", "pmcgrosschargeamt", "pmcnetchargeamt", "itemid", "itemname", "netchargeamt", "reqdate", "otherInfo")
    dbgDf
  }

}


