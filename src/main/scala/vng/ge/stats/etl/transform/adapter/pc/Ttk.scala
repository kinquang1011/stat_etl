package vng.ge.stats.etl.transform.adapter.pc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.DbgGameFormatter
import vng.ge.stats.etl.utils.{PathUtils}

import scala.util.Try
import scala.util.matching.Regex

/**
  * Created by lamnt6 on 05/05/2017.
  */
class Ttk extends DbgGameFormatter("ttk"){
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

  override def getActivityDs(logDate: String, hourly:String): DataFrame = {
    var loginRaw: DataFrame = null

    val getSid = udf { (path: String, date: String) => {
      var sid = "0"
      Try {
//        val date = logdate.replaceAll("-","")

        val pattern = new Regex("([0-9]{4}-[0-9]{2}-[0-9]{2}\\/log\\/ttk[0-9]{1,200})")

        val result = (pattern findAllIn path).mkString(",")
        if (!result.equalsIgnoreCase("")) {
          sid = result.replaceAll(date + "/"+"log"+"/", "")
        }

      }
      sid
    }
    }

    val _gameCode = gameCode

    if (hourly == "") {
      val patternPathLogin = Constants.GAMELOG_DIR + "/ttk/[yyyy-MM-dd]/log/*/login.txt"

      val loginPath = PathUtils.generateLogPathDaily(patternPathLogin, logDate)
      loginRaw = getCsvWithHeaderLog(loginPath,"\\t")

    }else {

    }
    val sf = Constants.FIELD_NAME

//    loginRaw = loginRaw.select(input_file_name().as("path"))
    loginRaw = loginRaw.withColumn(sf.SID,getSid(input_file_name,lit(logDate)))
    loginRaw = loginRaw.withColumn(sf.LOG_DATE,col("logdate"))
    loginRaw = loginRaw.withColumn(sf.ID,col("account"))
    loginRaw = loginRaw.withColumn(sf.IP,col("ip"))
    loginRaw = loginRaw.withColumn(sf.ACTION,lit("login"))
    loginRaw = loginRaw.withColumn(sf.GAME_CODE,lit(_gameCode))
    loginRaw = loginRaw.where($"logdate".contains(logDate))
    loginRaw = loginRaw.where("isfail = 0")

    loginRaw = loginRaw.select(sf.GAME_CODE,sf.ACTION,sf.LOG_DATE,sf.ID,sf.IP,sf.SID)



    loginRaw
  }

}