package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{FairyFormatter, Formatter}
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 17/10/2017.
  * Contact point : silp
  * release: 25/10/2017
  */
class Tlbbm extends Formatter ("tlbbm") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var paymentRaw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.WAREHOUSE_DIR + "/tlbbm/recharge/[yyyy-MM-dd]/*.gz"
      var path: Array[String] = null
      if(logDate.contains("2017-12-27")) {
        path = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        path = PathUtils.generateLogPathDaily(pattern, logDate)
      }
      paymentRaw = getRawLog(path)
    }else{

    }

    val paymentGenerate = (line: Array[String]) => {
      var amt = line(13).toLong * 200
      if(line(13) == "3000" && line(10) < "50"){
        amt = 500000L
      }
      amt.toString
    }


    val filter = (line:Array[String]) => {
      var rs = false
      if (line.length>=10 && (line(0)).startsWith(logDate) && !line(6).equalsIgnoreCase("23000")) {
        rs = true
      }
      rs
    }
    val sf = Constants.FIELD_NAME
    val paymentDs = paymentRaw.map(line => line.split("\\t")).filter(line => filter(line)).map { line =>
      val dateTime = line(0)
      val id = line(8)
      val roleId = id
      val sid = line(6)
      val gross =paymentGenerate(line)
      val net = paymentGenerate(line)

      ("tlbbm", dateTime,id,roleId,sid,gross,net)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.GROSS_AMT,sf.NET_AMT)

    paymentDs
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginRaw: RDD[String] = null
    if (hourly == "") {
      val pattern = Constants.WAREHOUSE_DIR + "/tlbbm/login/[yyyy-MM-dd]/*.gz"
      var path: Array[String] = null
      if(logDate.contains("2017-12-27")) {
        path = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        path = PathUtils.generateLogPathDaily(pattern, logDate)
      }
      loginRaw = getRawLog(path)
    }else{

    }
    val filterlogin = (line:Array[String]) => {
      var rs = false
      if (line.length>=10 && (line(0)).startsWith(logDate) && !line(6).equalsIgnoreCase("23000")) {
        rs = true
      }
      rs
    }

    val sf = Constants.FIELD_NAME
    val loginDs = loginRaw.map(line => line.split("\\t")).filter(line => filterlogin(line)).map { line =>
      val dateTime = line(0)
      val id = line(9)
      val roleId = id
      val sid = line(6)
      val action = "login"

      ("tlbbm", dateTime,id,roleId,sid,action)
    }
      .toDF(sf.GAME_CODE, sf.LOG_DATE, sf.ID,sf.RID,sf.SID,sf.ACTION)
    loginDs
  }

}
