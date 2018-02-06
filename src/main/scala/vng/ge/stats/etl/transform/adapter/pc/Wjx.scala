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
class Wjx extends DbgGameFormatter("wjx"){
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
    val patternNewReg = Constants.GAMELOG_DIR + "/gslog/wjx/[yyyy-MM-dd]/Account_Register.csv"
    val pathNewReg = PathUtils.generateLogPathDaily(patternNewReg, logDate)
    val rawNewReg = getRawLog(pathNewReg)
    val newRegFilter = (line:Array[String]) => {
      var rs = false
      val date = line(1)
      if (date.startsWith(logDate)&& !line(0).isEmpty) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME

    val newRegDf = rawNewReg.map(line => line.split("\\t")).filter(line => newRegFilter(line)).map { line =>
      val date = line(1)
      val accountName = line(0)
      ("wjx",date,accountName)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
    newRegDf
  }


  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val patternPathLogin = Constants.GAMELOG_DIR + "/gslog/wjx/[yyyy-MM-dd]/*/Login.csv"
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
      val id = line(1)
      val serverId = line(5)
      val action = "login"
      val level = line(4)

      ("wjx", dateTime,id,id,serverId,action,level)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.ACTION,sf.LEVEL)

    loginDs
  }

}
