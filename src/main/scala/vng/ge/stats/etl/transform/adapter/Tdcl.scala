package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 30/10/2017.
  */
//contact point : phucph
//alpha test: 14/11/2017


class Tdcl extends FairyFormatter("tdcl") {

  import org.apache.spark.sql.functions.lit
  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var joinDs: DataFrame = null
    if (hourly == "") {
      val patternlLogin = Constants.GAMELOG_DIR + "/tdcl/[yyyy-MM-dd]/datalog/loggame_[yyyyMMdd]/*/login_logout.csv"
      val loginPath = PathUtils.generateLogPathDaily(patternlLogin, logDate)
      val loginDs = getCsvWithHeaderLog(loginPath, "\t").select("state","server", "roleId", "ip", "time","channel")

      val patternlUserInfo = Constants.GAMELOG_DIR + "/tdcl/[yyyy-MM-dd]/datalog/loggame_[yyyyMMdd]/*/userinfo.csv"
      val infoPath = PathUtils.generateLogPathDaily(patternlUserInfo, logDate)
      val infoDs = getCsvWithHeaderLog(infoPath, "\t").select("name","id")

      joinDs = loginDs.as("a").join(infoDs.as("b"), loginDs("roleId") === infoDs("id"), "inner").where("b.name is not null")
      joinDs=joinDs.select("name","server","roleId","ip","time","state","channel")
    }
    val sf = Constants.FIELD_NAME
    joinDs = joinDs.withColumnRenamed("server", sf.SID)
      .withColumnRenamed("name", sf.ID)
      .withColumnRenamed("roleId", sf.RID)
      .withColumnRenamed("ip", sf.IP)
      .withColumnRenamed("time", sf.LOG_DATE)
      .withColumnRenamed("state", sf.ACTION)
      .withColumnRenamed("channel", sf.CHANNEL)
      .withColumn(sf.GAME_CODE,lit("tdcl"))

    joinDs=joinDs.where("log_date like '"+logDate+"%'");

    joinDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var joinDs: DataFrame = emptyDataFrame

    val setAmtWithRate = udf { (amt: String) => {
      var vnd: Double = 0
      vnd = amt.toDouble * 100
      vnd.toString
    }
    }

    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/tdcl/[yyyy-MM-dd]/datalog/loggame_[yyyyMMdd]/*/recharge.csv"
      val paymentPath = PathUtils.generateLogPathDaily(pattern, logDate)
      var paymentDs = getCsvWithHeaderLog(paymentPath, "\\t")
      if(paymentDs != emptyDataFrame){
        paymentDs=paymentDs.select("server","roleId", "gold", "channel", "time","order")
        val patternlUserInfo = Constants.GAMELOG_DIR + "/tdcl/[yyyy-MM-dd]/datalog/loggame_[yyyyMMdd]/*/userinfo.csv"
        val infoPath = PathUtils.generateLogPathDaily(patternlUserInfo, logDate)
        val infoDs = getCsvWithHeaderLog(infoPath, "\\t").select("name","id")

        joinDs = paymentDs.as("a").join(infoDs.as("b"), paymentDs("roleId") === infoDs("id"), "left_outer").where("b.name is not null")

        joinDs=joinDs.select("name","time","server","roleId","channel","gold","order")

        joinDs = joinDs.withColumnRenamed("server", sf.SID)
          .withColumnRenamed("name", sf.ID)
          .withColumnRenamed("roleId", sf.RID)
          .withColumnRenamed("time", sf.LOG_DATE)
          .withColumnRenamed("channel", sf.CHANNEL)
          .withColumnRenamed("order", sf.TRANS_ID)
          .withColumn(sf.GAME_CODE,lit("tdcl"))

        joinDs=joinDs.withColumn(sf.GROSS_AMT,setAmtWithRate(col("gold")))
        joinDs=joinDs.withColumn(sf.NET_AMT,setAmtWithRate(col("gold")))

      }
    }
    joinDs=joinDs.where("log_date like '"+logDate+"%'").dropDuplicates(sf.TRANS_ID);
    joinDs;

  }
}
