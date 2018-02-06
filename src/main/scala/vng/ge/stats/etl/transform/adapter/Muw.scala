package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, TmpFormatter}
import vng.ge.stats.etl.utils.{Common, PathUtils}


/**
  * Created by quangctn on 25/09/2017.
  * trungn2
  * code ccu:
  * alpha test : 14/10
  */
class Muw extends FairyFormatter("muw") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)

    this -> run() -> close()
  }

  /* LOGIN
  list_id : _c0
  roleid : _c1
  uid : _c2
  time : _c3
  osid : _c4
  sid : _c5
  level : _c6
  map : _c7
  ip : _c8
  channel : _c9
  LOGOUT
  list_id: _c0
  roleid: _c1
  uid: _c2
  time: _c3
  osid: _c4
  sid: _c5
  level: _c6
  map: _c7
  onlinetime: _c8
  ip: _c9
  channel: _c10
   */
  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs, logoutDs: DataFrame = null
    val sf = Constants.FIELD_NAME
    if (hourly == "") {
      val loginPattern = Constants.GAMELOG_DIR + "/muw/[yyyy-MM-dd]/login_s*.csv"
      val logoutPattern = Constants.GAMELOG_DIR + "/muw/[yyyy-MM-dd]/logout_s*.csv"
      val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate, 1)
      val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate, 1)
      loginDs = getCsvLog(loginPath, ",")
        .selectExpr("_c1 as rid", "_c2 as id", "_c3 as log_date", "_c5 as sid", "_c6 as level", "_c8 as ip")
        .withColumn(sf.ONLINE_TIME, lit(0))
        .withColumn(sf.ACTION, lit("login"))
      logoutDs = getCsvLog(logoutPath, ",")
        .selectExpr("_c1 as rid", "_c2 as id", "_c3 as log_date", "_c5 as sid", "_c6 as level", "_c8 as online_time", "_c9 as ip")
        .withColumn(sf.ACTION, lit("logout"))
    }
    loginDs.union(logoutDs).where(s"log_date like '%$logDate%'")
      .withColumn(sf.GAME_CODE, lit("muw"))
  }

  /* format recharge
      list_id : _c0
      roleid : _c1
      uid : _c2
      time : _c3
      osid : _c4
      sid : _c5
      level : _c6
      amount : _c7
      money : _c8
      balance : _c9
      channel : _c10
      online : _c11
      order : _c12
      first : _c13
      loginchannel : _c14

     */
  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var payDs: DataFrame = null
    if (hourly == "") {
      val payPattern = Constants.GAMELOG_DIR + "/muw/[yyyy-MM-dd]/recharge_s*.csv"
      val payPath = PathUtils.generateLogPathDaily(payPattern, logDate, 1)
      payDs = getCsvLog(payPath, ",").select("_c1", "_c2", "_c3", "_c5", "_c7", "_c8", "_c12")
    }
    val sf = Constants.FIELD_NAME
    payDs = payDs.withColumnRenamed("_c1", sf.RID)
      .withColumnRenamed("_c2", sf.ID)
      .withColumnRenamed("_c3", sf.LOG_DATE)
      .withColumnRenamed("_c5", sf.SID)
      .withColumnRenamed("_c12", sf.TRANS_ID)
    if (logDate < "2017-10-18") {
      payDs = payDs.withColumn(sf.GROSS_AMT, col("_c7") * 200)
    } else {
      payDs = payDs.withColumn(sf.GROSS_AMT, col("_c8"))
    }
    payDs = payDs.withColumn(sf.NET_AMT, col(sf.GROSS_AMT))
    payDs = payDs.where(s"log_date like '%$logDate%' and trans_id != '9999999999'").drop("_c7", "_c8").withColumn(sf.GAME_CODE, lit("muw"))
    payDs

  }

  /* REGISTER
    list_id: _c0
    roleid: _c1
    uid: _c2
    time: _c3
    osid: _c4
    sid: _c5
    ip: _c6
    role_cls: _c7
    channel: _c8

   */
  /* override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
     val sf = Constants.FIELD_NAME
     var registerDs: DataFrame = null
     val regisPattern = Constants.GAMELOG_DIR +"/muw/[yyyy-MM-dd]/register_s*.csv"
     val regisPath = PathUtils.generateLogPathDaily(regisPattern,logDate,1)
     registerDs = getCsvLog(regisPath, ",")
       .selectExpr("_c1 as rid", "_c2 as id", "_c3 as log_date", "_c5 as sid")
     registerDs = registerDs.withColumn(sf.GAME_CODE,lit("muw")).where(s"log_date like '%$logDate%'")
     registerDs

   }*/
}

/*
  ACTIVE = LOGIN+ UNION LOGOUT
  LOGIN+ = LOGIN + CHARACTER (JOIN BY RID)
*/
/*override def oldActivityDs(logDate: String, hourly: String): DataFrame = {

  var loginRaw, logoutRaw, characterLog: RDD[String] = null

  val code = "muw"
  if (hourly == "") {
    val loginPattern = Constants.GAMELOG_DIR + s"/$code/[yyyy-MM-dd]/login_s*.csv"
    val logoutPattern = Constants.GAMELOG_DIR + s"/$code/[yyyy-MM-dd]/logout_s*.csv"
    val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate, 1)
    val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate, 1)
    loginRaw = getRawLog(loginPath)
    logoutRaw = getRawLog(logoutPath)
    val characterPattern = Constants.GAMELOG_DIR + s"/$code/[yyyy-MM-dd]/character_s*.csv"
    val characterPath = PathUtils.generateLogPathDaily(characterPattern, logDate, 1)
    characterLog = getRawLog(characterPath)
  }
  val trimQuote = (s: String) => {
    s.replace("\"", "")
  }
  val filterLogin = (s: Array[String]) => {
    var rs = false
    if (s.length >= 3) {
      val newDate = logDate.replace("-", "")

      if (!(trimQuote(s(2)).equalsIgnoreCase(""))) {
        if ( /*(s(0).startsWith(newDate)) &&*/ (trimQuote(s(2)) forall Character.isDigit)) {
          rs = true
        }
      }
    }
    rs
  }

  val filterLogout = (s: Array[String]) => {
    var rs = false
    if (s.length >= 5) {

      /* if (s(4).startsWith(logDate)) {*/
      rs = true
      /* }*/
    }
    rs
  }
  val filterCharacter = (s: Array[String]) => {
    s.length >= 3
  }
  val sf = Constants.FIELD_NAME

  val loginDs = loginRaw.map(line => line.split(",")).filter(line => filterLogin(line)).map { r =>
    val rid = trimQuote(r(1))
    val datetime = MyUdf.timestampToDate(trimQuote(r(2)).toLong * 1000)
    (rid, datetime, "login")
  }.toDF(sf.RID, sf.LOG_DATE, sf.ACTION)


  val characterDs = characterLog.map(line => line.split(",")).filter(line => filterCharacter(line)).map { r =>
    val rid = trimQuote(r(0))
    val sid = trimQuote(r(1))
    val id = trimQuote(r(2))
    (rid, sid, id)
  }.toDF("ridC", sf.SID, sf.ID).dropDuplicates("ridC")

  val join = loginDs.as('a).join(characterDs.as('b), loginDs("rid") === characterDs("ridC"), "left_outer")
    .where("ridC is not null")
    .select(sf.RID, sf.SID, sf.ID, sf.LOG_DATE, sf.ACTION)

  val logoutDs = logoutRaw.map(line => line.split(",")).filter(line => filterLogout(line)).map { r =>
    val rid = trimQuote(r(0))
    val sid = trimQuote(r(1))
    val id = trimQuote(r(2))
    val datetime = trimQuote(r(4))
    (rid, sid, id, datetime, "logout")
  }.toDF(sf.RID, sf.SID, sf.ID, sf.LOG_DATE, sf.ACTION).dropDuplicates(sf.RID)
  join.union(logoutDs).withColumn(sf.GAME_CODE, lit(code))
}*/


/*def oldPaymentDs(logDate: String, hourly: String): DataFrame = {
 var revRaw: RDD[String] = null
 if (hourly == "") {
   val revPattern = Constants.GAMELOG_DIR + "/muw/[yyyy-MM-dd]/money_income_s*csv"
   val revPath = PathUtils.generateLogPathDaily(revPattern, logDate, 1)
   revRaw = getRawLog(revPath, true)
 }
 val trimQuote = (str: String) => {
   str.replace("\"", "")
 }
 val paymentFilter = (arr: Array[String]) => {
   var rs = false
   if (arr.length >= 8) {
     Common.logger(": "+trimQuote(arr(6)))
     if ( /*(arr(7).startsWith(logDate)) &&*/ trimQuote(arr(6)).equalsIgnoreCase("260"))
       rs = true
   }
   rs
 }
 val getSid = (path: String) => {
   var sid = ""
   Try {
     Common.logger("Path :" + path)
     val pattern = new Regex("money_income_s(\\d+)")
     val result = (pattern findAllIn path).mkString(",")
     if (!result.equalsIgnoreCase("")) {
       sid = result.replace("money_income_s  ", "")
     }

   }
   sid
 }
 val sf = Constants.FIELD_NAME
 val revDs = revRaw.map(line => line.split("\\|"))
   .filter(line => paymentFilter(line(1).split(","))).map { r =>
   val sid = getSid(r(0))
   val data = r(1).split(",")
   val acc = trimQuote(data(2))
   val gross = trimQuote(data(3)).toInt * 100
   val net = gross
   val dateTime = trimQuote(data(7))
   ("muw", dateTime, acc, gross, net, sid)
 }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT, sf.SID)
 revDs
}*/

