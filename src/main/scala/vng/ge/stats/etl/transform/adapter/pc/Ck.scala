package vng.ge.stats.etl.transform.adapter.pc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{PathUtils}

/**
  * Created by lamnt6 on 05/05/2017.
  */
class Ck extends DbgGameFormatter("ck"){
  //must set AppId array
  setWarehouseDir(Constants.WAREHOUSE_DIR)
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
      val patternPathLogin = Constants.WAREHOUSE_DIR + "/ck/login_logout/[yyyy-MM-dd]/*.gz"
      val loginPath = PathUtils.generateLogPathDaily(patternPathLogin, logDate)
      loginRaw = getRawLog(loginPath)
    }else {

    }

    val filterlog = (line:Array[String]) => {
      var rs = false
      if(line.length==7){
        if (line(4).startsWith(logDate) || line(5).startsWith(logDate)){
          rs = true
        }
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlog(line)).map { line =>
      val action = "login"
      var dateTime = line(5)
      if (line(4).startsWith(logDate)){
        dateTime = line(4)
      }else if(line(5).startsWith(logDate)){
        dateTime = line(5)
      }
      val id = line(1)
      val sid = line(6)
      val rid = line(2)
      ("ck", dateTime,id,rid,sid,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.ACTION)

    loginDs
  }


  override def getCcuDs(logDate: String, hourly: String): DataFrame = {
    import sparkSession.implicits._
    val patternPath = Constants.WAREHOUSE_DIR + "/ck/ccu/[yyyy-MM-dd]/*.gz"
    val path = PathUtils.generateLogPathDaily(patternPath, logDate)
    val raw = getRawLog(path)

    val ccuFilter = (line:Array[String]) => {
      var rs = false
      if (line(0).startsWith(logDate)) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val rsDs = raw.map(line => line.split("\\t")).filter(line => ccuFilter(line)).map { line =>
      val ccu = line(2).toInt
      val date = line(0)
      val sid = line(1)
      ("ck",date, ccu,sid)
    }.toDF(sf.GAME_CODE, sf.LOG_DATE, sf.CCU,sf.SID)

    rsDs
  }

}