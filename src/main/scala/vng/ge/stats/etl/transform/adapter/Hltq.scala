package vng.ge.stats.etl.transform.adapter

  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.DataFrame
  import vng.ge.stats.etl.constant.Constants
  import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
  import vng.ge.stats.etl.transform.udf.MyUdf
  import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

  /**
    * Created by lamnt6 on 12/12/2017
    */
  class Hltq extends FairyFormatter ("hltq") {
    import sqlContext.implicits._

    def start(args: Array[String]): Unit = {
      initParameters(args)
      this -> run -> close
    }

    override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
      var paymentRaw: RDD[String] = null
      var payingPath: Array[String] = null
      if (hourly == "") {
        val payingPattern = Constants.GAMELOG_DIR + "/hltq/[yyyy-MM-dd]/datalog/HLTQ_GAMELOG*/hltq-charge*.log"
        if(logDate.contains("2017-12-11")) {
          payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate,1)
        }else{
          payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate)
        }
        paymentRaw = getRawLog(payingPath)
      }else {
        val payingPattern = Constants.GAMELOG_DIR + "/hltq/[yyyy-MM-dd]/datalog/HLTQ_GAMELOG*/hltq-charge*.log"
        payingPath = PathUtils.generateLogPathDaily(payingPattern, logDate,1)
        paymentRaw = getRawLog(payingPath)
      }


      val getDateTime = (timestamp:String) => {
        var rs = timestamp
        if(timestamp!=null){
          rs = DateTimeUtils.getDate(timestamp.toLong*1000)
        }
        rs
      }

      val filterLog = (line: Array[String]) =>{
        var rs = false
        if (line.length>=14 && (line(6) forall Character.isDigit) && getDateTime(line(6)).startsWith(logDate) && line(12) == "1"){
          rs = true
        }
        rs
      }

      val sf = Constants.FIELD_NAME
      val paymentDs = paymentRaw.map(line => line.split("\\|")).filter(line => filterLog(line)).map { line =>
        val id = line(0)
        val roleId = line(1)
        val roleName = line(2)
        val dateTime = getDateTime(line(6))
        val serverId = line(11)
        val gross = line(7)
        val net = line(7)
        val transactionId = line(13)
        //      val roleId = line(14)
        ("hltq", dateTime,id,roleId,roleName,serverId,transactionId,gross,net)
      }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.ROLE_NAME,sf.SID,sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT)
      paymentDs


    }

    override def getActivityDs(logDate: String, hourly:String): DataFrame = {
      var loginRaw: RDD[String] = null
      var logoutRaw: RDD[String] = null
      var logoutPath: Array[String] = null

      if (hourly == "") {
        val loginPattern = Constants.GAMELOG_DIR + "/hltq/[yyyy-MM-dd]/datalog/HLTQ_GAMELOG_*/hltq-rolelogin-*.log"
        val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
        loginRaw = getRawLog(loginPath)

        val logoutPattern = Constants.GAMELOG_DIR + "/hltq/[yyyy-MM-dd]/datalog/HLTQ_GAMELOG_*/hltq-rolelogout-*.log"
        if(logDate.contains("2017-12-11")) {
          logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate,1)
        }else{
          logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
        }
        logoutRaw = getRawLog(logoutPath)
      }else {
        val loginPattern = Constants.GAMELOG_DIR + "/hltq/[yyyy-MM-dd]/datalog/HLTQ_GAMELOG_*/hltq-rolelogin-*.log"
        val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
        loginRaw = getRawLog(loginPath)

        val logoutPattern = Constants.GAMELOG_DIR + "/hltq/[yyyy-MM-dd]/datalog/HLTQ_GAMELOG_*/hltq-rolelogout-*.log"
        logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate,1)
        logoutRaw = getRawLog(logoutPath)
      }
      val getDateTime = (timestamp:String) => {
        var rs = timestamp
        if(timestamp!=null){
          rs = DateTimeUtils.getDate(timestamp.toLong*1000)
        }
        rs
      }

      val filterlog = (line: Array[String]) => {
        var rs = false
        if (line.length>=11  && (line(6) forall Character.isDigit) && getDateTime(line(6)).startsWith(logDate) && line(8).equalsIgnoreCase("SUCCESS")){
          rs = true
        }
        rs
      }
      val filterlogout = (line: Array[String]) => {
        var rs = false
        if (line.length>=12  && (line(6) forall Character.isDigit) && getDateTime(line(6)).startsWith(logDate) && line(9).equalsIgnoreCase("SUCCESS")){
          rs = true
        }
        rs
      }

      val getOs = (s: String) => {
        var rs = "other"
        if (s.toLowerCase.equals("android")) {
          rs = "android"
        }else if (s.toLowerCase.equals("ios")) {
          rs = "ios"
        }
        rs
      }



      val sf = Constants.FIELD_NAME
      val loginDs = loginRaw.map(line => line.split("\\|")).filter(line => filterlog(line)).map { line =>
        val dateTime = getDateTime(line(6))
        val id = line(0)
        val serverId = line(7)
        val roleId = line(1)
        val roleName = line(2)
        val os =getOs(line(10))
        val action = "login"
        ("hltq", dateTime,id,serverId,roleId,roleName,os,action)
      }
        .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.ACTION)

      val logoutDs = logoutRaw.map(line => line.split("\\|")).filter(line => filterlogout(line)).map { line =>
        val dateTime = getDateTime(line(6))
        val id = line(0)
        val serverId = line(8)
        val roleId = line(1)
        val roleName = line(2)
        val os =getOs(line(11))
        val action = "logout"
        ("hltq", dateTime,id,serverId,roleId,roleName,os,action)
      }
        .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.RID,sf.ROLE_NAME,sf.OS,sf.ACTION)

      val ds: DataFrame = loginDs.union(logoutDs)
      ds
    }
  }
