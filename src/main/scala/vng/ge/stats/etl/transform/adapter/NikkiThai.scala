package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, substring, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, FairySdkFormatter, Formatter}
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.transform.udf.MyUdf.dateTimeIncrement
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils}

/**
  * Created by quangctn on 13/04/2017.
  */
class NikkiThai extends FairySdkFormatter("nikkithai") {

  import sqlContext.implicits._

  val convertRate = 650

  def start(args: Array[String]): Unit = {
    initParameters(args)
    setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)
    this -> run -> close
  }
  private val makeOtherIfNull = udf { (str: String) => {

    if (str == null || str == "") "other" else str
  }
  }
  private val lowerCaseCol = udf { (os: String) => {
    if (os != null) {
      os.toLowerCase
    } else {
      os
    }
  }
  }
  private val fixRevenue = udf { (rev: Long) => {
    var newRev = rev;
    val fix1 = 5.000 * 650
    val fix2 = 9000 * 650
    val fix3 = 15000 * 650
    val fix4 = 30000 * 650
    val fix5 = 50000 * 650
    if(rev == fix1 || rev == fix2 || rev == fix3|| rev == fix4|| rev == fix5){
      newRev = rev/100
    }
    newRev
  }
  }
  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginLogoutDs: DataFrame = null
    if (hourly == "") {

      val sdkSource = "sdk_sea"
      val sdkGameCode = "NIKKITHAI"
      val logPattern = Constants.GAME_LOG_DIR + "/" + sdkSource + "/[yyyy-MM-dd]/" + sdkGameCode + "_Login_InfoLog/" + sdkGameCode + "_Login_InfoLog-[yyyy-MM-dd].gz"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
      loginLogoutDs = getJsonLog(logPath)
      if (loginLogoutDs.rdd.isEmpty()) {
        loginLogoutDs = createEmptyActivityDs()
      } else {
        loginLogoutDs = loginLogoutDs.withColumn("rid", col("userID"))
        loginLogoutDs = loginLogoutDs.withColumn("os", makeOtherIfNull(col("os")))
        loginLogoutDs = loginLogoutDs.withColumn("device_os", lowerCaseCol(col("device_os")))
        loginLogoutDs = loginLogoutDs.withColumn("gameCode", lit(gameCode))
        loginLogoutDs = loginLogoutDs.withColumn("action", lit("login"))
        loginLogoutDs = loginLogoutDs.withColumn("updatetime",logDateWithTimeZone(col("updatetime"), lit("0")))
        loginLogoutDs = loginLogoutDs.filter($"updatetime".contains(logDate))
          .select("device", "userID", "type", "updatetime", "package_name", "device_id", "gameCode", "action", "os", "device_os", "rid")

        val sf = Constants.FIELD_NAME

        loginLogoutDs = loginLogoutDs.toDF(sf.DEVICE, sf.ID, sf.CHANNEL, sf.LOG_DATE, sf.PACKAGE_NAME, sf.DID, sf.GAME_CODE, sf.ACTION, sf.OS_VERSION, sf.OS, sf.RID)

      }
    } else {
      var loginLogoutRaw: RDD[String] = null
      val logPattern = Constants.GAME_LOG_DIR + "/nikkithai/[yyyyMMdd]/loggame/ntlog.*"
      val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
      loginLogoutRaw = getRawLog(logPath)


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
        if (line.length >= 25 && line(2).startsWith(logDate) &&
          (line(0) == "PlayerLogin" || line(0) == "PlayerLogout")) {
          rs = true
        }
        rs
      }
      val sf = Constants.FIELD_NAME
      loginLogoutDs = loginLogoutRaw.map(line => line.split("\\|")).filter(line => filterLoginLogout(line)).map { line =>
        val id = line(6)
        val serverId = line(1)
        val dateTime = line(2)
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
        ("nikkithai", dateTime, serverId, action, id, level, did, os, device, loginChannel, onlineTime, carrier, network, resolution)
      }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ACTION, sf.ID, sf.LEVEL, sf.DID, sf.OS, sf.DEVICE, sf.CHANNEL, sf.ONLINE_TIME, sf.CARRIER, sf.NETWORK, sf.RESOLUTION)
    }
    loginLogoutDs
  }
  private val logDateWithTimeZone = udf { (datetime: String, timezone: String) => {
    var gmt = 7
    gmt = gmt - timezone.toInt
    val time = dateTimeIncrement(datetime, gmt * 3600)
    time
  }
  }
  //Nikki_Logcharge_2017-02-07.csv.gz
  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentDs: DataFrame = null
    //DAILY ETL
    if (hourly == "") {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      val pathGameAdd = s"/ge/gamelogs/sdk_sea/{$oneDayAgo,$logDate}/Log_NIKKITHAI_GameAdd/*.gz"
      val gameAddRaw = getJsonLog(pathGameAdd)
      val pathDbgAdd = s"/ge/gamelogs/sdk_sea/{$oneDayAgo,$logDate}/Log_NIKKITHAI_DBGAdd/*.gz"
      val dbgAddRaw = getJsonLog(pathDbgAdd)

      var raw = dbgAddRaw.as('a).join(gameAddRaw.as('b), dbgAddRaw("transactionID") === gameAddRaw("transactionID"), "left_outer").where("a.resultCode == '1'")
        .select("a.updatetime", "a.gameID", "a.userID", "a.transactionID", "a.pmcID", "pmcNetChargeAmt", "pmcGrossChargeAmt", "a.roleID", "b.itemID")
      raw = raw.withColumn("updatetime", logDateWithTimeZone(col("updatetime"), lit("0")))

      val convertMap = Map(
        "com.pg2.nikkithai.diamond38" -> 35,
        "com.pg2.nikkithai.diamond90" -> 50,
        "com.pg2.nikkithai.diamond188" -> 100,
        "com.pg2.nikkithai.diamond377" -> 200,
        "com.pg2.nikkithai.diamond578" -> 300,
        "com.pg2.nikkithai.diamond968" -> 500,
        "com.pg2.nikkithai.diamond1968" -> 1000,
        "com.pg2.nikkithai.diamond4188" -> 2000,
        "com.pg2.nikkithai.gift" -> 30,
        "com.pg2.nikkithai.packchangenameW"->  300 ,
        "com.pg2.nikkithai.packstarlightW"->  100 ,
        "com.pg2.nikkithai.packstaminaW"->  100 ,
        "com.pg2.nikkithai.packlargestarlightW"->  300 ,
        "com.pg2.nikkithai.packlargestaminaW"->  300 ,
        "com.pg2.nikkithai.giftdoubleseven"->  30 ,
        "com.pg2.nikkithai.pack99a"->30,
        "com.pg2.nikkithai.pack99b"->30,
        "com.pg2.nikkithai.pack99c"->30,
        "com.pg2.nikkithai.pack99d"->30,
        "com.pg2.nikkithai.pack99e"->30,
        "com.pg2.nikkithai.pack99f"->30,
        "com.pg2.nikkithai.pack199a"->50,
        "com.pg2.nikkithai.pack199b"->50,
        "com.pg2.nikkithai.pack199c"->50,
        "com.pg2.nikkithai.pack199d"->50,
        "com.pg2.nikkithai.pack199e"->50,
        "com.pg2.nikkithai.pack199f"->50,
        "com.pg2.nikkithai.pack299a"->70,
        "com.pg2.nikkithai.pack299b"->70,
        "com.pg2.nikkithai.pack299c"->70,
        "com.pg2.nikkithai.pack299d"->70,
        "com.pg2.nikkithai.pack299e"->70,
        "com.pg2.nikkithai.pack299f"->70,
        "com.pg2.nikkithai.pack399a"->100,
        "com.pg2.nikkithai.pack399b"->100,
        "com.pg2.nikkithai.pack399c"->100,
        "com.pg2.nikkithai.pack399d"->100,
        "com.pg2.nikkithai.pack399e"->100,
        "com.pg2.nikkithai.pack399f"->100,
        "com.pg2.nikkithai.pack499a"->140,
        "com.pg2.nikkithai.pack499b"->140,
        "com.pg2.nikkithai.pack499c"->140,
        "com.pg2.nikkithai.pack499d"->140,
        "com.pg2.nikkithai.pack499e"->140,
        "com.pg2.nikkithai.pack499f"->140,
        "com.pg2.nikkithai.pack599a"->165,
        "com.pg2.nikkithai.pack599b"->165,
        "com.pg2.nikkithai.pack599c"->165,
        "com.pg2.nikkithai.pack599d"->165,
        "com.pg2.nikkithai.pack599e"->165,
        "com.pg2.nikkithai.pack599f"->165,
        "com.pg2.nikkithai.pack699a"->200,
        "com.pg2.nikkithai.pack699b"->200,
        "com.pg2.nikkithai.pack699c"->200,
        "com.pg2.nikkithai.pack699d"->200,
        "com.pg2.nikkithai.pack699e"->200,
        "com.pg2.nikkithai.pack699f"->200,
        "com.pg2.nikkithai.pack799a"->230,
        "com.pg2.nikkithai.pack799b"->230,
        "com.pg2.nikkithai.pack799c"->230,
        "com.pg2.nikkithai.pack999a"->300,
        "com.pg2.nikkithai.pack999b"->300,
        "com.pg2.nikkithai.pack999c"->300,
        "com.pg2.nikkithai.pack1099a"->340,
        "com.pg2.nikkithai.pack1099b"->340,
        "com.pg2.nikkithai.pack1099c"->340,
        "com.pg2.nikkithai.pack1599a"->500,
        "com.pg2.nikkithai.pack1599b"->500,
        "com.pg2.nikkithai.pack1599c"->500,
        "com.pg2.nikkithai.pack2099a"->670,
        "com.pg2.nikkithai.pack2099b"->670,
        "com.pg2.nikkithai.pack2099c"->670,
        "com.pg2.nikkithai.pack3099a"->1000,
        "com.pg2.nikkithai.pack3099b"->1000,
        "com.pg2.nikkithai.pack3099c"->1000
      )
      //App Store, Google Play Channel
      //      val convertMapStore = Map(
      //        "com.pg2.nikkithai.diamond38" -> 69,
      //        "com.pg2.nikkithai.diamond90" -> 99,
      //        "com.pg2.nikkithai.diamond188" -> 139,
      //        "com.pg2.nikkithai.diamond377" -> 249,
      //        "com.pg2.nikkithai.diamond578" -> 349,
      //        "com.pg2.nikkithai.diamond968" -> 559,
      //        "com.pg2.nikkithai.diamond1968" -> 1100,
      //        "com.pg2.nikkithai.diamond4188" -> 2300,
      //        "com.pg2.nikkithai.gift" -> 35,
      //        "com.pg2.nikkithai.packchangename" -> 349,
      //        "com.pg2.nikkithai.packstarlight" -> 139,
      //        "com.pg2.nikkithai.packstamina" -> 139,
      //        "com.pg2.nikkithai.packlargestarlight" -> 349,
      //        "com.pg2.nikkithai.packlargestamina" -> 349,
      //        "com.pg2.nikkithai.giftdoubleseven" -> 35,
      //        "com.pg2.nikkithai.pack99a"->35,
      //        "com.pg2.nikkithai.pack99b"->35,
      //        "com.pg2.nikkithai.pack99c"->35,
      //        "com.pg2.nikkithai.pack99d"->35,
      //        "com.pg2.nikkithai.pack99e"->35,
      //        "com.pg2.nikkithai.pack99f"->35,
      //        "com.pg2.nikkithai.pack199a"->69,
      //        "com.pg2.nikkithai.pack199b"->69,
      //        "com.pg2.nikkithai.pack199c"->69,
      //        "com.pg2.nikkithai.pack199d"->69,
      //        "com.pg2.nikkithai.pack199e"->69,
      //        "com.pg2.nikkithai.pack199f"->69,
      //        "com.pg2.nikkithai.pack299a"->99,
      //        "com.pg2.nikkithai.pack299b"->99,
      //        "com.pg2.nikkithai.pack299c"->99,
      //        "com.pg2.nikkithai.pack299d"->99,
      //        "com.pg2.nikkithai.pack299e"->99,
      //        "com.pg2.nikkithai.pack299f"->99,
      //        "com.pg2.nikkithai.pack399a"->139,
      //        "com.pg2.nikkithai.pack399b"->139,
      //        "com.pg2.nikkithai.pack399c"->139,
      //        "com.pg2.nikkithai.pack399d"->139,
      //        "com.pg2.nikkithai.pack399e"->139,
      //        "com.pg2.nikkithai.pack399f"->139,
      //        "com.pg2.nikkithai.pack499a"->179,
      //        "com.pg2.nikkithai.pack499b"->179,
      //        "com.pg2.nikkithai.pack499c"->179,
      //        "com.pg2.nikkithai.pack499d"->179,
      //        "com.pg2.nikkithai.pack499e"->179,
      //        "com.pg2.nikkithai.pack499f"->179,
      //        "com.pg2.nikkithai.pack599a"->209,
      //        "com.pg2.nikkithai.pack599b"->209,
      //        "com.pg2.nikkithai.pack599c"->209,
      //        "com.pg2.nikkithai.pack599d"->209,
      //        "com.pg2.nikkithai.pack599e"->209,
      //        "com.pg2.nikkithai.pack599f"->209,
      //        "com.pg2.nikkithai.pack699a"->249,
      //        "com.pg2.nikkithai.pack699b"->249,
      //        "com.pg2.nikkithai.pack699c"->249,
      //        "com.pg2.nikkithai.pack699d"->249,
      //        "com.pg2.nikkithai.pack699e"->249,
      //        "com.pg2.nikkithai.pack699f"->249,
      //        "com.pg2.nikkithai.pack799a"->279,
      //        "com.pg2.nikkithai.pack799b"->279,
      //        "com.pg2.nikkithai.pack799c"->279,
      //        "com.pg2.nikkithai.pack999a"->349,
      //        "com.pg2.nikkithai.pack999b"->349,
      //        "com.pg2.nikkithai.pack999c"->349,
      //        "com.pg2.nikkithai.pack1099a"->389,
      //        "com.pg2.nikkithai.pack1099b"->389,
      //        "com.pg2.nikkithai.pack1099c"->389,
      //        "com.pg2.nikkithai.pack1599a"->559,
      //        "com.pg2.nikkithai.pack1599b"->559,
      //        "com.pg2.nikkithai.pack1599c"->559,
      //        "com.pg2.nikkithai.pack2099a"->739,
      //        "com.pg2.nikkithai.pack2099b"->739,
      //        "com.pg2.nikkithai.pack2099c"->739,
      //        "com.pg2.nikkithai.pack3099a"->1100,
      //        "com.pg2.nikkithai.pack3099b"->1100,
      //        "com.pg2.nikkithai.pack3099c"->1100
      //      )

      val convertMapStore = Map(
        "com.pg2.nikkithai.diamond38"->69,
        "com.pg2.nikkithai.diamond90"->99,
        "com.pg2.nikkithai.diamond188"->139,
        "com.pg2.nikkithai.diamond377"->249,
        "com.pg2.nikkithai.diamond578"->349,
        "com.pg2.nikkithai.diamond968"->559,
        "com.pg2.nikkithai.diamond1968"->1100,
        "com.pg2.nikkithai.diamond4188"->2300,
        "com.pg2.nikkithai.gift"->35,
        "com.pg2.nikkithai.packchangename"->349,
        "com.pg2.nikkithai.packstarlight"->139,
        "com.pg2.nikkithai.packstamina"->139,
        "com.pg2.nikkithai.packlargestarlight"->349,
        "com.pg2.nikkithai.packlargestamina"->349,
        "com.pg2.nikkithai.giftdoubleseven"->35,
        "com.pg2.nikkithai.packluckstamina"->35,
        "com.pg2.nikkithai.packlucklargestamina"->69,
        "com.pg2.nikkithai.packluckcoin"->35,
        "com.pg2.nikkithai.packlucklargecoin"->69,
        "com.pg2.nikkithai.pack99a"->35,
        "com.pg2.nikkithai.pack99b"->35,
        "com.pg2.nikkithai.pack99c"->35,
        "com.pg2.nikkithai.pack99d"->35,
        "com.pg2.nikkithai.pack99e"->35,
        "com.pg2.nikkithai.pack99f"->35,
        "com.pg2.nikkithai.pack199a"->69,
        "com.pg2.nikkithai.pack199b"->69,
        "com.pg2.nikkithai.pack199c"->69,
        "com.pg2.nikkithai.pack199d"->69,
        "com.pg2.nikkithai.pack199e"->69,
        "com.pg2.nikkithai.pack199f"->69,
        "com.pg2.nikkithai.pack299a"->99,
        "com.pg2.nikkithai.pack299b"->99,
        "com.pg2.nikkithai.pack299c"->99,
        "com.pg2.nikkithai.pack299d"->99,
        "com.pg2.nikkithai.pack299e"->99,
        "com.pg2.nikkithai.pack299f"->99,
        "com.pg2.nikkithai.pack399a"->139,
        "com.pg2.nikkithai.pack399b"->139,
        "com.pg2.nikkithai.pack399c"->139,
        "com.pg2.nikkithai.pack399d"->139,
        "com.pg2.nikkithai.pack399e"->139,
        "com.pg2.nikkithai.pack399f"->139,
        "com.pg2.nikkithai.pack499a"->179,
        "com.pg2.nikkithai.pack499b"->179,
        "com.pg2.nikkithai.pack499c"->179,
        "com.pg2.nikkithai.pack499d"->179,
        "com.pg2.nikkithai.pack499e"->179,
        "com.pg2.nikkithai.pack499f"->179,
        "com.pg2.nikkithai.pack599a"->209,
        "com.pg2.nikkithai.pack599b"->209,
        "com.pg2.nikkithai.pack599c"->209,
        "com.pg2.nikkithai.pack599d"->209,
        "com.pg2.nikkithai.pack599e"->209,
        "com.pg2.nikkithai.pack599f"->209,
        "com.pg2.nikkithai.pack699a"->249,
        "com.pg2.nikkithai.pack699b"->249,
        "com.pg2.nikkithai.pack699c"->249,
        "com.pg2.nikkithai.pack699d"->249,
        "com.pg2.nikkithai.pack699e"->249,
        "com.pg2.nikkithai.pack699f"->249,
        "com.pg2.nikkithai.pack799a"->279,
        "com.pg2.nikkithai.pack799b"->279,
        "com.pg2.nikkithai.pack799c"->279,
        "com.pg2.nikkithai.pack999a"->349,
        "com.pg2.nikkithai.pack999b"->349,
        "com.pg2.nikkithai.pack999c"->349,
        "com.pg2.nikkithai.pack1099a"->389,
        "com.pg2.nikkithai.pack1099b"->389,
        "com.pg2.nikkithai.pack1099c"->389,
        "com.pg2.nikkithai.pack1599a"->559,
        "com.pg2.nikkithai.pack1599b"->559,
        "com.pg2.nikkithai.pack1599c"->559,
        "com.pg2.nikkithai.pack2099a"->739,
        "com.pg2.nikkithai.pack2099b"->739,
        "com.pg2.nikkithai.pack2099c"->739,
        "com.pg2.nikkithai.pack3099a"->1100,
        "com.pg2.nikkithai.pack3099b"->1100,
        "com.pg2.nikkithai.pack3099c"->1100,
        "com.pg2.nikkithai.pack99g"->35,
        "com.pg2.nikkithai.pack99h"->35,
        "com.pg2.nikkithai.pack99i"->35,
        "com.pg2.nikkithai.pack99j"->35,
        "com.pg2.nikkithai.pack99k"->35,
        "com.pg2.nikkithai.pack99l"->35,
        "com.pg2.nikkithai.pack99m"->35,
        "com.pg2.nikkithai.pack99n"->35,
        "com.pg2.nikkithai.pack99o"->35,
        "com.pg2.nikkithai.pack99p"->35,
        "com.pg2.nikkithai.pack199g"->69,
        "com.pg2.nikkithai.pack199h"->69,
        "com.pg2.nikkithai.pack199i"->69,
        "com.pg2.nikkithai.pack199j"->69,
        "com.pg2.nikkithai.pack199k"->69,
        "com.pg2.nikkithai.pack199l"->69,
        "com.pg2.nikkithai.pack199m"->69,
        "com.pg2.nikkithai.pack199n"->69,
        "com.pg2.nikkithai.pack199o"->69,
        "com.pg2.nikkithai.pack199p"->69,
        "com.pg2.nikkithai.pack299g"->99,
        "com.pg2.nikkithai.pack299h"->99,
        "com.pg2.nikkithai.pack299i"->99,
        "com.pg2.nikkithai.pack299j"->99,
        "com.pg2.nikkithai.pack299k"->99,
        "com.pg2.nikkithai.pack299l"->99,
        "com.pg2.nikkithai.pack299m"->99,
        "com.pg2.nikkithai.pack299n"->99,
        "com.pg2.nikkithai.pack299o"->99,
        "com.pg2.nikkithai.pack299p"->99,
        "com.pg2.nikkithai.pack399g"->139,
        "com.pg2.nikkithai.pack399h"->139,
        "com.pg2.nikkithai.pack399i"->139,
        "com.pg2.nikkithai.pack399j"->139,
        "com.pg2.nikkithai.pack399k"->139,
        "com.pg2.nikkithai.pack399l"->139,
        "com.pg2.nikkithai.pack399m"->139,
        "com.pg2.nikkithai.pack399n"->139,
        "com.pg2.nikkithai.pack399o"->139,
        "com.pg2.nikkithai.pack399p"->139,
        "com.pg2.nikkithai.pack99q"->35,
        "com.pg2.nikkithai.pack99r"->35,
        "com.pg2.nikkithai.pack99s"->35,
        "com.pg2.nikkithai.pack99t"->35,
        "com.pg2.nikkithai.pack99u"->35,
        "com.pg2.nikkithai.pack99v"->35,
        "com.pg2.nikkithai.pack99w"->35,
        "com.pg2.nikkithai.pack99x"->35,
        "com.pg2.nikkithai.pack99y"->35,
        "com.pg2.nikkithai.pack99z"->35,
        "com.pg2.nikkithai.pack199q"->69,
        "com.pg2.nikkithai.pack199r"->69,
        "com.pg2.nikkithai.pack199s"->69,
        "com.pg2.nikkithai.pack199t"->69,
        "com.pg2.nikkithai.pack199u"->69,
        "com.pg2.nikkithai.pack199v"->69,
        "com.pg2.nikkithai.pack199w"->69,
        "com.pg2.nikkithai.pack199x"->69,
        "com.pg2.nikkithai.pack199y"->69,
        "com.pg2.nikkithai.pack199z"->69
      )
      val _convertRate = convertRate
      val getGross = (itemId: String) => {
        var vnd: Double = 0
        var bac: Double = 0
        if (convertMapStore.contains(itemId)) {
          bac = convertMapStore(itemId)
        }
        vnd = bac * _convertRate
        vnd.toLong
      }

      val sf = Constants.FIELD_NAME
      paymentDs = raw.rdd.map(line => {
        val dateTime = line.getString(0)
        val id = line.getString(2)
        val tid = line.getString(3)
        val pmcId = line.getString(4)
        val netAmt = line.getLong(5) * _convertRate
        val itemId = line.getString(8)
        var grossAmt = line.getLong(6) * _convertRate
        if (itemId != null && pmcId.toLowerCase.contains("store")) {
          grossAmt = getGross(itemId)
        }
        ("nikkithai", dateTime, id, tid, netAmt, grossAmt, id, pmcId)
      }).toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.TRANS_ID, sf.NET_AMT, sf.GROSS_AMT, sf.RID, sf.CHANNEL)
      paymentDs = paymentDs.withColumn("newDate", substring(col(sf.LOG_DATE), 0, 10))
      if(logDate.equalsIgnoreCase("2018-01-26")){
      paymentDs = paymentDs
        .withColumn(sf.NET_AMT,fixRevenue(col(sf.NET_AMT)))
        .withColumn(sf.GROSS_AMT,fixRevenue(col(sf.GROSS_AMT)))
      }
      paymentDs = paymentDs.where(s"newDate like '$logDate'")
    }
    //HOURLY ETL
    else {
      var raw: RDD[String] = null
      val logPattern = Constants.GAME_LOG_DIR + "/nikkithai/[yyyyMMdd]/chargelog_minly/Nikki_Logcharge*.csv"
      val logPath = PathUtils.generateLogPathHourly(logPattern, logDate)
      raw = getRawLog(logPath)
      val trimQuote = (s: String) => {
        s.replace("\"", "")
      }

      val firstFilter = (line: Array[String]) => {
        var rs = false
        if (line.length >= 17 && line(11).startsWith(logDate)) {
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
        "com.pg2.nikkithai.diamond38" -> 35,
        "com.pg2.nikkithai.diamond90" -> 50,
        "com.pg2.nikkithai.diamond188" -> 100,
        "com.pg2.nikkithai.diamond377" -> 200,
        "com.pg2.nikkithai.diamond578" -> 300,
        "com.pg2.nikkithai.diamond968" -> 500,
        "com.pg2.nikkithai.diamond1968" -> 1000,
        "com.pg2.nikkithai.diamond4188" -> 2000,
        "com.pg2.nikkithai.gift" -> 30
      )
      val _convertRate = convertRate

      val getGross = (s: String) => {
        val ss = s.split("-")
        var vnd: Double = 0
        if (ss.length >= 5) {
          val itemId = ss(4)
          var bac: Double = 0
          if (convertMap.contains(itemId)) {
            bac = convertMap(itemId)
          }
          vnd = bac * _convertRate
        }
        vnd
      }
      val sf = Constants.FIELD_NAME
      paymentDs = raw.map(line => line.split("\",\"")).filter(line => firstFilter(line)).map { line =>
        val trans = trimQuote(line(2))
        val id = trimQuote(line(3))
        val money = line(5)
        val os = getOs(line(15))
        val dateTime = line(11)
        val netRev = money.toInt * _convertRate
        val grossRev = getGross(trimQuote(line(0)))
        ("nikkithai", dateTime, id, os, netRev, grossRev, trans)
      }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.OS, sf.NET_AMT, sf.GROSS_AMT, sf.TRANS_ID)
      paymentDs = paymentDs.dropDuplicates(sf.TRANS_ID)
    }
    paymentDs

  }


  /*override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    var raw: RDD[String] = null
    if (hourly == "") {
      val logPattern = Constants.GAME_LOG_DIR + "/mrtg/server_ccu.[yyyyMMdd]"
      val logPath = PathUtils.generateLogPathDaily(logPattern, logDate, numberOfDay = 1)
      raw = getRawLog(logPath)
    }
    val gameFilter = (line: Array[String]) => {
      var rs = false
      if (line.length == 3 && line(0).toLowerCase.startsWith("nikkithai")) {
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
      val timeStamp = line(2).toLong * 1000
      val dateTime = MyUdf.timestampToDate(timeStamp)
      ("nikkithai", serverId, ccu, dateTime)
    }.toDF(sf.GAME_CODE, sf.SID, sf.CCU, sf.LOG_DATE)
    ccuDs
  }*/

  def getAbcDs(logDate: String, spark: SparkSession): DataFrame = {
    val pathGameAdd = s"/ge/gamelogs/sdk_sea/$logDate/Log_NIKKITHAI_GameAdd/*.gz"
    val gameAddRaw = spark.read.json(pathGameAdd)
    val pathDbgAdd = s"/ge/gamelogs/sdk_sea/$logDate/Log_NIKKITHAI_DBGAdd/*.gz"
    val dbgAddRaw = spark.read.json(pathDbgAdd)

    val raw = dbgAddRaw.as('a).join(gameAddRaw.as('b), dbgAddRaw("transactionID") === gameAddRaw("transactionID"), "left_outer")
      .select("a.updatetime", "a.gameID", "a.userID", "a.transactionID", "a.pmcID", "pmcNetChargeAmt", "pmcGrossChargeAmt", "a.roleID", "b.itemID")
    raw
  }
}
