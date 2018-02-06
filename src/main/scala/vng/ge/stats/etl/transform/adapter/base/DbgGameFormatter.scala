package vng.ge.stats.etl.transform.adapter.base

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.udf.MyUdf.changeReportTime
import vng.ge.stats.etl.utils.{Common, PathUtils}

/**
  * Created by canhtq on 26/04/2017.
  */
class DbgGameFormatter(_gameCode:String)  extends FairyFormatter ("pcgame"){
  gameCode= _gameCode


  /***
	Field	Description
0	transid	định danh giao dịch. Là 1 số long gồm 17 số, 8 số đầu tiên là ngày giao dịch theo định dạng yyyyMMdd
1	appid	định danh product
2	userid	định danh user
3	platform
4	flow
5	serverid
6	reqdate
7	itemid
8	itemname
9	itemquantity
10	chargeamt
11	feclientid
12	env
13	pmcid
14	pmctransid
15	pmcgrosschargeamt
16	pmcnetchargeamt
17	pmcchargeresult
18	pmcupddate
19	appnotifyresult
20	appupddate
21	isretrytonotify
22	noretrytonotify
23	lastretrytonotify
24	hostname
25	hostdate
26	transstatus	Trạng thái giao dịch. Status=='SUCCESSFUL': giao dịch thành công
27	step
28	stepresult
29	isprocess
30	transphase
31	exceptions
32	apptransid
33	netchargeamt	tiền tính TopPayingUser
34	addinfo
35	YMD
36	FileName
    */


  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    import sparkSession.implicits._

    val logPattern = Constants.GAME_LOG_DIR + "/dbg/[yyyy-MM-dd]"
    val logPath = PathUtils.generateLogPathDaily(logPattern, logDate)
    val transDate = logDate.replace("-","")
    //val cnfApps = _apps.toDF("app_id")
    val raw = getRawLog(logPath)
    val tDate = logDate.replace("-","")
    val dbgAppCnfPath =  "/ge/fairy/master_data/directbilling_apps.csv"
    val cnfDs = sparkSession.read.option("delimiter",",").option("header","true").csv(dbgAppCnfPath)
    val cnfApps = cnfDs.where("game_code='" + gameCode +"'")

    val appsFilter = (line: Array[String]) => {
      var rs = false

      if(line.length>33){
        rs = line(26).equalsIgnoreCase("SUCCESSFUL")
        if(rs){
          val tid = line(0)
          rs = tid.startsWith(tDate)
        }
      }
      /*else {
        //Common.logger("line = " + line.mkString("###"))
      }*/
      rs
    }
    val dbgDf = raw.map(line => line.split("\\t")).filter(line => appsFilter(line)).map { line =>
      val transid = line(0)
      val appid = line(1)
      val id = line(2)
      val sid = line(5)
      val gross_amt = line(15)
      val net_amt = line(33)
      val log_date = line(23)
      (transid,log_date, id, id, sid, appid, net_amt, gross_amt)
    }.toDF("transid","log_date", "id", "rid", "sid", "appid", "net_amt", "gross_amt")

    val sf = Constants.FIELD_NAME
    val rsDf = dbgDf.as('dbg).join(cnfApps.as('cnf),dbgDf("appid")===cnfApps("appid")).withColumn("game_code",lit(gameCode)).withColumn("trans_date",substring(col("transid"),0,8)).where("trans_date='"+transDate+"'").select("game_code","log_date","id","rid","net_amt","gross_amt","transid")
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID, sf.NET_AMT, sf.GROSS_AMT, sf.TRANS_ID)
    val finDs = rsDf.sort(sf.TRANS_ID).dropDuplicates(sf.TRANS_ID)
    finDs
  }

  def createTotalData(logDate:String): Unit ={
    val spark = sparkSession
    import spark.implicits._
    val rawNRU = spark.read.text("/ge/gamelogs/dba/KPIREPORT/EXPORT_NRU_A_CANHTQ_1.csv.tar.gz")
    val nruFilters = (line: Array[String]) => {
      var rs = false
      if(line.length<=2){
        Common.logger("line = " + line.mkString("###"))
      }else{
        rs=true
      }

      rs
    }
    var nruDs = rawNRU.map(line => line.getString(0).split(",")).filter(line => nruFilters(line)).map { line =>
      val ogc =line.apply(0)

      var gc =ogc
      if(ogc.startsWith("EXPORT")){
        gc="WTVN"
      }
      val acn =line.apply(1)
      val regdate =line.apply(2)
      (gc.toLowerCase,regdate,acn)
    }.toDF("game_code","log_date","id")
    if(gameCode.equalsIgnoreCase("tlbbw")){
      nruDs = spark.read.option("header","true").csv("tmp/TLBBW_2.txt").selectExpr("ACCOUNT_NAME as id","from_unixtime(unix_timestamp(REGISTER_DATE,'MM/dd/yyyy hh:mm:ss'),'yyyy-MM-dd HH:mm:ss') as log_date").withColumn("game_code",lit("tlbbw")).sort("id").dropDuplicates("id")
    }

    val gameNRU = nruDs.withColumn("date",substring(col("log_date"),0,10)).where("game_code='" + gameCode +"'").where("date<='" + logDate +"'").select("game_code","log_date","id")
    store(gameNRU, logDate, Constants.LOG_TYPE.TOTAL_ACC_LOGIN)

    val rawPU = spark.read.text("/ge/gamelogs/dba/KPIREPORT/EXPORT_NPU_A_CANHTQ_1.csv.tar.gz")
    val puDs = rawPU.map(line => line.getString(0).split("\\,")).filter(line => nruFilters(line)).map { line =>
      val ogc =line.apply(0)
      var gc =ogc
      if(ogc.startsWith("EXPORT")){
        gc="JX1"
      }
      val acn =line.apply(1)
      val regdate =line.apply(2)
      (gc.toLowerCase,regdate,acn)
    }.toDF("game_code","log_date","id")
    val gameNPU = puDs.withColumn("date",substring(col("log_date"),0,10)).where("game_code='" + gameCode +"'").where("date<='" + logDate +"'").select("game_code","log_date","id")
    store(gameNPU, logDate, Constants.LOG_TYPE.TOTAL_ACC_PAID)

  }
}
