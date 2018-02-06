package vng.ge.stats.etl.transform.adapter.pc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{PathUtils}
import scala.util.Try
import scala.util.matching.Regex

/**
  * Created by canhtq on 05/05/2017.
  */
class Ckck extends DbgGameFormatter("ckck"){
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    if("buildTotalData".equalsIgnoreCase(_logType)){
      this->createTotalData(_logDate)
    }else{
      this -> run -> close
    }
  }

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val patternPath = Constants.GAMELOG_DIR + "/ckck/[yyyy-MM-dd]/datalog/loggame_[yyyyMMdd]/*_loginSuccessLog.txt"
      val loginPath = PathUtils.generateLogPathDaily(patternPath, logDate)
      loginRaw = getRawLog(loginPath, true, ",")

    }

    val getSid = (path: String, date: String) => {
      var sid = "0"
      Try {
        //        val date = logdate.replaceAll("-","")
        val pattern = new Regex("([0-9]{4}-[0-9]{2}-[0-9]{2}\\/datalog/loggame_[0-9]{4}[0-9]{2}[0-9]{2}\\/s[0-9]{1,200})")
        val result = (pattern findAllIn path).mkString(",")
        if (!result.equalsIgnoreCase("")) {
          sid = result.replaceAll(date + "/datalog/loggame_[0-9]{4}[0-9]{2}[0-9]{2}/", "")
        }
      }
      sid
    }


    val filterlog = (line: Array[String]) => {
      var rs = false
      val loginDate = line(3)
      if (loginDate.startsWith(logDate)) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginDf = loginRaw.map(line => line.split(",")).filter(line => filterlog(line)).map { line =>
      val action = "login"
      val dateTime = line(3)
      val id = line(2)
      val sid = getSid(line(0), logDate)
      ("ckck", dateTime,id,sid,action)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.SID,sf.ACTION)
    loginDf

  }




}
