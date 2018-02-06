package vng.ge.stats.etl.transform.adapter.pc

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{DateTimeUtils, PathUtils}

/**
  * Created by lamnt6 on 05/05/2017.
  */
class Ttl3d extends DbgGameFormatter("ttl3d"){
  //must set AppId array
  import sqlContext.implicits._


  def start(args: Array[String]): Unit = {
    initParameters(args)
    if("buildTotalData".equalsIgnoreCase(_logType)){
      this->createTotalData(_logDate)
    }else{
      this -> run -> close
    }
  }
  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    import sparkSession.implicits._
    val patternNewReg = Constants.GAMELOG_DIR + "/gslog/ks/[yyyy-MM-dd]/*/register.*.csv"
    val pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate)
    val rawNewReg = getRawLog(pathNewReg)
    val newRegFilter = (line:Array[String]) => {
      var rs = false
      if (line.length>=3 && line(0).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME

    val newRegDf = rawNewReg.map(line => line.split("\\t")).filter(line => newRegFilter(line)).map { line =>
      val date = line(0)
      val accountName = line(2)
      val sid = line(1)

      ("tll3d",date,accountName,sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID)
    newRegDf
  }


  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val patternPathLogin = Constants.GAMELOG_DIR + "/gslog/ks/[yyyy-MM-dd]/*/login_logout.*.csv"
      val loginPath = PathUtils.generateLogPathDaily(patternPathLogin, logDate)
      loginRaw = getRawLog(loginPath)
    }else {
      //      val loginPattern = Constants.GAMELOG_DIR + "/cube/[yyyy-MM-dd]/LOGIN/LOGIN-[yyyy-MM-dd]_*"
      //      val loginPath = PathUtils.generateLogPathHourly(loginPattern, logDate)
      //      loginRaw = getRawLog(loginPath)
    }

    val filterlog = (line:Array[String]) => {
      var rs = false
      if(line(0).startsWith(logDate)){
        rs = true

      }


      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val dateTime = line(0)
      val id = line(2)
      val serverId = line(1)
      val action = "login"

      ("tll3d", dateTime,id,id,serverId,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.ACTION)

    loginDs
  }

}
