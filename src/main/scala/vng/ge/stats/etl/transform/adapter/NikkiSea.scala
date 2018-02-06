package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.Formatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils}

/**
  * Created by tuonglv on 16/01/2017.
  */
class NikkiSea extends Formatter("nikkisea") {

  import sqlContext.implicits._

  val timeIncr: Long = -3600
  val convertRate = 22500

  def start(args: Array[String]): Unit = {
    initParameters(args)
    setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)
    this -> run -> close
  }

  override def otherETL(logDate: String, hourly: String, logType: String) {
    if (logType == "") {
      //onlyInNikkiSea(logDate, hourly)
    } else {
      if (logType == "abcxyz") {
        //abczyz()
      }
    }
  }

  def abczyz(): Unit = {
    Common.logger("aaa")
  }

  def onlyInNikkiSea(logDate: String, hourly: String): Unit = {
    val ds: DataFrame = null
    /**
      * todo
      */
    val _gameCode = gameCode
    val outputPath = PathUtils.getParquetPath(_gameCode, logDate,
      rootDir = Constants.FAIRY_WAREHOUSE_DIR,
      sourceFolder = "data",
      dataFolder = "xyz")
    storeInParquet(ds, outputPath, schema = null)
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    val _timeIncr = timeIncr
    var loginLogoutRaw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/loggame/ntlog.log-[yyyyMMdd].gz"
      var logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      val nextDayFromLogDate = DateTimeUtils.getDateDifferent(1, logDate, Constants.TIMING, Constants.A1)
      ///ge/gamelogs/nikkisea/20170213/loggame/ntlog.log-2017021301
      val getOneMoreHour = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/loggame/ntlog.log*"
      val logPathOneMoreHour = PathUtils.generateLogPathDaily(getOneMoreHour, nextDayFromLogDate, numberOfDay = 1)
      logPath = logPath ++ logPathOneMoreHour
      loginLogoutRaw = getRawLog(logPath)
    } else {
      val logPattern = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/loggame/ntlog.*"
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
      ("nikkisea", dateTime, serverId, action, id, level, did, os, device, loginChannel, onlineTime, carrier, network, resolution)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ACTION, sf.ID, sf.LEVEL, sf.DID, sf.OS, sf.DEVICE, sf.CHANNEL, sf.ONLINE_TIME, sf.CARRIER, sf.NETWORK, sf.RESOLUTION)
    loginLogoutDs
  }

  //Nikki_Logcharge_2017-02-07.csv.gz
  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val _timeIncr = timeIncr
    var raw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/log_recharge/Nikki_Logcharge_[yyyy-MM-dd].csv.gz"
      var logPath = PathUtils.generateLogPathDaily(logPattern, logDate)

      val nextDayFromLogDate = DateTimeUtils.getDateDifferent(1, logDate, Constants.TIMING, Constants.A1)
      val currentDate = DateTimeUtils.getCurrentDate()

      var logPathOneMoreHour = Array[String]()
      if (nextDayFromLogDate == currentDate) {
        // neu ngay xu ly log la yesterday, tuc la report daily
        ///ge/gamelogs/nikkisea/20170213/chargelog_hourly/Nikki_Logcharge_2017-02-13-01.csv
        val getOneMoreHour = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/chargelog_hourly/Nikki_Logcharge_[yyyy-MM-dd]-01.csv"
        logPathOneMoreHour = PathUtils.generateLogPathDaily(getOneMoreHour, nextDayFromLogDate, numberOfDay = 1)
      } else {
        // truong hop chay lai
        val getOneMoreHour = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/log_recharge/Nikki_Logcharge_[yyyy-MM-dd].csv.gz"
        logPathOneMoreHour = PathUtils.generateLogPathDaily(getOneMoreHour, nextDayFromLogDate, numberOfDay = 1)
      }
      logPath = logPath ++ logPathOneMoreHour
      raw = getRawLog(logPath)
    } else {
      val logPattern = Constants.GAME_LOG_DIR + "/nikkisea/[yyyyMMdd]/chargelog_minly/Nikki_Logcharge_*.csv"
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
    //Fix get data 07,08/06/2017
    val fixGetOs = (s: String) => {
      val osId = s.substring(s.length - 1, s.length)
      var rs = getOs(osId)
      rs

    }

    val convertMap = Map(
      "com.pg2.nikkisea.diamond120" -> 1.99,
      "com.pg2.nikkisea.diamond188" -> 2.99,
      "com.pg2.nikkisea.diamond377" -> 5.99,
      "com.pg2.nikkisea.diamond668" -> 9.99,
      "com.pg2.nikkisea.diamond1368" -> 19.99,
      "com.pg2.nikkisea.diamond4288" -> 59.99,
      "com.pg2.nikkisea.diamond7288" -> 99.99,
      "com.pg2.nikkisea.gift" -> 0.99,
      "com.pg2.nikkisea.packluckstamina" -> 0.99,
      "com.pg2.nikkisea.packlucklargestamina" -> 1.99,
      "com.pg2.nikkisea.packluckcoin" -> 0.99,
      "com.pg2.nikkisea.packlucklargecoin" -> 1.99,
      "com.pg2.nikkisea.packdiscount" -> 0.99,
      "com.pg2.nikkisea.packchangename" -> 9.99,
      "com.pg2.nikkisea.packstarlight" -> 12.99,
      "com.pg2.nikkisea.packstamina" -> 12.99,
      "com.pg2.nikkisea.packlargestarlight" -> 24.99,
      "com.pg2.nikkisea.packlargestamina" -> 24.99,
      "com.pg2.nikkisea.packshoplimitpack" -> 3.99,
      "com.pg2.nikkisea.packbluedragon" -> 0.99,
      "com.pg2.nikkisea.packwhitetiger" -> 0.99,
      "com.pg2.nikkisea.packphoenix" -> 0.99,
      "com.pg2.nikkisea.packblacktortoise" -> 0.99,
      "com.pg2.nikkisea.packlanternfestival" -> 1.99,
      "com.pg2.nikkisea.packcoustominze1" -> 3.99,
      "com.pg2.nikkisea.packcoustominze2" -> 3.99,
      "com.pg2.nikkisea.packring" -> 14.99,
      "com.pg2.nikkisea.pack99a" -> 0.99,
      "com.pg2.nikkisea.pack99b" -> 0.99,
      "com.pg2.nikkisea.pack99c" -> 0.99,
      "com.pg2.nikkisea.pack199a" -> 1.99,
      "com.pg2.nikkisea.pack199b" -> 1.99,
      "com.pg2.nikkisea.pack199c" -> 1.99,
      "com.pg2.nikkisea.pack299a" -> 2.99,
      "com.pg2.nikkisea.pack299b" -> 2.99,
      "com.pg2.nikkisea.pack299c" -> 2.99,
      "com.pg2.nikkisea.pack399a" -> 3.99,
      "com.pg2.nikkisea.pack399b" -> 3.99,
      "com.pg2.nikkisea.pack399c" -> 3.99,
      "com.pg2.nikkisea.pack499a" -> 4.99,
      "com.pg2.nikkisea.pack499b" -> 4.99,
      "com.pg2.nikkisea.pack499c" -> 4.99,
      "com.pg2.nikkisea.pack599a" -> 5.99,
      "com.pg2.nikkisea.pack599b" -> 5.99,
      "com.pg2.nikkisea.pack599c" -> 5.99,
      "com.pg2.nikkisea.pack699a" -> 6.99,
      "com.pg2.nikkisea.pack699b" -> 6.99,
      "com.pg2.nikkisea.pack699c" -> 6.99,
      "com.pg2.nikkisea.pack999a" -> 9.99,
      "com.pg2.nikkisea.pack999b" -> 9.99,
      "com.pg2.nikkisea.pack999c" -> 9.99,
      "com.pg2.nikkisea.pack1099a" -> 10.99,
      "com.pg2.nikkisea.pack1099b" -> 10.99,
      "com.pg2.nikkisea.pack1099c" -> 10.99,
      "com.pg2.nikkisea.pack1299a" -> 12.99,
      "com.pg2.nikkisea.pack1299b" -> 12.99,
      "com.pg2.nikkisea.pack1299c" -> 12.99,
      "com.pg2.nikkisea.pack1499a" -> 14.99,
      "com.pg2.nikkisea.pack1499b" -> 14.99,
      "com.pg2.nikkisea.pack1499c" -> 14.99,
      "com.pg2.nikkisea.pack1699a" -> 16.99,
      "com.pg2.nikkisea.pack1699b" -> 16.99,
      "com.pg2.nikkisea.pack1699c" -> 16.99,
      "com.pg2.nikkisea.pack1999a" -> 19.99,
      "com.pg2.nikkisea.pack1999b" -> 19.99,
      "com.pg2.nikkisea.pack1999c" -> 19.99,
      "com.pg2.nikkisea.pack2999a" -> 29.99,
      "com.pg2.nikkisea.pack2999b" -> 29.99,
      "com.pg2.nikkisea.pack2999c" -> 29.99,
      "com.pg2.nikkisea.pack4999a" -> 49.99,
      "com.pg2.nikkisea.pack4999b" -> 49.99,
      "com.pg2.nikkisea.pack4999c" -> 49.99,
      "com.pg2.nikkisea.pack5999a" -> 59.99,
      "com.pg2.nikkisea.pack5999b" -> 59.99,
      "com.pg2.nikkisea.pack5999c" -> 59.99,
      "com.pg2.nikkisea.pack6999a" -> 69.99,
      "com.pg2.nikkisea.pack6999b" -> 69.99,
      "com.pg2.nikkisea.pack6999c" -> 69.99,
      "com.pg2.nikkisea.pack9999a" -> 99.99,
      "com.pg2.nikkisea.pack9999b" -> 99.99,
      "com.pg2.nikkisea.pack9999c" -> 99.99,
      "com.pg2.nikkisea.pack99x" -> 0.99,
      "com.pg2.nikkisea.pack99y" -> 0.99,
      "com.pg2.nikkisea.pack99z" -> 0.99,
      "com.pg2.nikkisea.pack99d" -> 0.99,
      "com.pg2.nikkisea.pack99e" -> 0.99,
      "com.pg2.nikkisea.pack99f" -> 0.99,
      "com.pg2.nikkisea.pack99g" -> 0.99,
      "com.pg2.nikkisea.pack99h" -> 0.99,
      "com.pg2.nikkisea.pack99i" -> 0.99,
      "com.pg2.nikkisea.pack99j" -> 0.99,
      "com.pg2.nikkisea.pack99k" -> 0.99,
      "com.pg2.nikkisea.pack99l" -> 0.99,
      "com.pg2.nikkisea.pack99m" -> 0.99,
      "com.pg2.nikkisea.pack99n" -> 0.99,
      "com.pg2.nikkisea.pack99o" -> 0.99,
      "com.pg2.nikkisea.pack99p" -> 0.99,
      "com.pg2.nikkisea.pack99q" -> 0.99,
      "com.pg2.nikkisea.pack99r" -> 0.99,
      "com.pg2.nikkisea.pack99s" -> 0.99,
      "com.pg2.nikkisea.pack99t" -> 0.99,
      "com.pg2.nikkisea.pack99u" -> 0.99,
      "com.pg2.nikkisea.pack99v" -> 0.99,
      "com.pg2.nikkisea.pack99w" -> 0.99,
      "com.pg2.nikkisea.pack199d" -> 1.99,
      "com.pg2.nikkisea.pack199e" -> 1.99,
      "com.pg2.nikkisea.pack199f" -> 1.99,
      "com.pg2.nikkisea.pack199g" -> 1.99,
      "com.pg2.nikkisea.pack199g" -> 1.99,
      "com.pg2.nikkisea.pack199h" -> 1.99,
      "com.pg2.nikkisea.pack199h" -> 1.99,
      "com.pg2.nikkisea.pack199i" -> 1.99,
      "com.pg2.nikkisea.pack199j" -> 1.99,
      "com.pg2.nikkisea.pack199k" -> 1.99,
      "com.pg2.nikkisea.pack199l" -> 1.99,
      "com.pg2.nikkisea.pack199m" -> 1.99,
      "com.pg2.nikkisea.pack199n" -> 1.99,
      "com.pg2.nikkisea.pack299d" -> 2.99,
      "com.pg2.nikkisea.pack299e" -> 2.99,
      "com.pg2.nikkisea.pack299f" -> 2.99,
      "com.pg2.nikkisea.pack299g" -> 2.99,
      "com.pg2.nikkisea.pack299h" -> 2.99,
      "com.pg2.nikkisea.pack299i" -> 2.99,
      "com.pg2.nikkisea.pack299j" -> 2.99,
      "com.pg2.nikkisea.pack299k" -> 2.99,
      "com.pg2.nikkisea.pack299l" -> 2.99,
      "com.pg2.nikkisea.pack399d" -> 3.99,
      "com.pg2.nikkisea.pack399e" -> 3.99,
      "com.pg2.nikkisea.pack399f" -> 3.99,
      "com.pg2.nikkisea.pack399g" -> 3.99,
      "com.pg2.nikkisea.pack399h" -> 3.99,
      "com.pg2.nikkisea.pack399i" -> 3.99,
      "com.pg2.nikkisea.pack399j" -> 3.99,
      "com.pg2.nikkisea.pack399k" -> 3.99,
      "com.pg2.nikkisea.pack599d" -> 5.99,
      "com.pg2.nikkisea.pack599e" -> 5.99,
      "com.pg2.nikkisea.pack599f" -> 5.99,
      "com.pg2.nikkisea.pack999d" -> 9.99,
      "com.pg2.nikkisea.pack999e" -> 9.99,
      "com.pg2.nikkisea.pack999f" -> 9.99,
      "com.pg2.nikkisea.pack599g" -> 5.99,
      "com.pg2.nikkisea.pack599h" -> 5.99,
      "com.pg2.nikkisea.pack599i" -> 5.99,
      "com.pg2.nikkisea.pack599j" -> 5.99,
      "com.pg2.nikkisea.pack599k" -> 5.99,
      "com.pg2.nikkisea.pack1499d" -> 14.99,
      "com.pg2.nikkisea.pack1499e" -> 14.99,
      "com.pg2.nikkisea.pack1499f"-> 14.99,
      "com.pg2.nikkisea.pack1499g" -> 14.99
    )
    val _convertRate = convertRate

    val getGross = (s: String) => {
      val ss = s.split("-")
      var vnd: Double = 0
      if (ss.length >= 5) {
        val itemId = ss(4)
        var usd: Double = 0
        if (convertMap.contains(itemId)) {
          usd = convertMap(itemId)
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
      var os = ""
      if (logDate.startsWith("2017-06-07") || logDate.startsWith("2017-06-08")) {
        os = fixGetOs(line(0))
      } else {
        os = getOs(line(15))
      }
      val dateTime = MyUdf.dateTimeIncrement(line(11), _timeIncr)

      val netRev = money
      val grossRev = getGross(trimQuote(line(0)))
      ("nikkisea", dateTime, id, os, netRev, grossRev, trans)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.OS, sf.NET_AMT, sf.GROSS_AMT, sf.TRANS_ID)
    paymentDs
  }

  ///ge/gamelogs/mrtg/server_ccu.20170207
  //NIKKISEA_02:0:1486486446
  /*override def getCcuDs(logDate: String, hourly: String): DataFrame = {
      var raw: RDD[String] = null
      if (hourly == "") {
          val logPattern = Constants.GAME_LOG_DIR + "/mrtg/server_ccu.[yyyyMMdd]"
          val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
          raw = getRawLog(logPath)
      }
      val gameFilter = (line: Array[String]) => {
          var rs = false
          if (line.length == 3 && line(0).toLowerCase.startsWith("nikkisea")) {
              if (line(0).split("_").length == 2) {
                  rs = true
              }
          }
          rs
      }
      //val spark = sparkSession
      //spark.read.json("/ge/gamelogs/nikkisea/20170419/log_ccu/ccu_nikki_20170420_0100.log").show()
      var sf = Constants.FIELD_NAME
      val ccuDs = raw.map(line => line.split(":")).filter(line => gameFilter(line)).map { line =>
          val t = line(0).split("_")
          val serverId = t(1)
          val ccu = line(1)
          val timeStamp = line(2).toLong
          val dateTime = MyUdf.timestampToDate(timeStamp*1000)
          ("nikkisea", serverId, ccu, dateTime)
      }.toDF(sf.GAME_CODE, sf.SID, sf.CCU, sf.LOG_DATE)
      ccuDs
  }*/
}
