package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, min, udf}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by lamnt6 on 30/10/2017.
  */
//contact point : HieuTM
//release test: 15/12/2017


class Ygzl extends FairyFormatter("ygzl") {

  import org.apache.spark.sql.functions.lit

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run -> close
  }

  override def getIdRegisterDs(logDate: String, _activityDs: DataFrame, _totalAccLoginDs: DataFrame): DataFrame = {
    var registerDs: DataFrame = null
    val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/[yyyyMMdd]/Log/dailyzalo/*/*_tb_user_[yyyyMMdd].csv"
    val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate,1)
    registerDs = getCsvLog(registerPath, ",")
    val sf = Constants.FIELD_NAME


    //    val setUserId = udf { (id: String, server:String) => {
    //      val userId = id+"_"+server
    //      userId
    //    }
    //    }

    var registerF=registerDs.groupBy("_c3").agg(min("_c28").as("_c28")).where("_c28 like'"+logDate +"%'")
    registerF = registerF.withColumnRenamed("_c3", sf.ID)
      .withColumnRenamed("_c28", sf.LOG_DATE)
      .withColumn(sf.GAME_CODE,lit("ygh5"))

    registerF=registerF.select(sf.ID,sf.LOG_DATE,sf.GAME_CODE)

    var join = registerF.as('a).join(registerDs.as('b), registerF(sf.ID) === registerDs("_c3") && registerF(sf.LOG_DATE) === registerDs("_c28"), "inner")
      .select(sf.ID,sf.LOG_DATE,sf.GAME_CODE,"_c2")

    join=join.withColumnRenamed("_c2", sf.SID)

    join
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var loginDs: DataFrame = null

    val sf = Constants.FIELD_NAME

    if (hourly == "") {

      val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/[yyyyMMdd]/Log/dailyzalo/*/roletb_user_bk_[yyyyMMdd].csv"
      val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate,1)
      loginDs = getCsvLog(registerPath, ",")


    }
    loginDs=loginDs.select("_c3","_c0","_c2","_c6","_c28","_c27").where("_c2 != '10000' and _c6 is not null")
    loginDs = loginDs.withColumnRenamed("_c3", sf.ID)
      .withColumnRenamed("_c2", sf.SID)
      .withColumnRenamed("_c6", sf.ROLE_NAME)
      .withColumn(sf.RID, col(sf.ID))
      .withColumnRenamed("_c27", sf.LOG_DATE)
      .withColumn(sf.GAME_CODE,lit("ygzl"))
      .withColumn(sf.ACTION,lit("login"))
    loginDs=loginDs.where("log_date like '"+logDate+"%'")
    loginDs=loginDs.select(sf.ID,sf.RID,sf.SID,sf.LOG_DATE,sf.ACTION,sf.GAME_CODE,sf.ROLE_NAME)

    loginDs
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    var paymentDs: DataFrame = emptyDataFrame
    var registerDs: DataFrame = null

    val setAmtWithRate = udf { (amt: String) => {
      val convertMap = Map(
        1 -> 20000,
        2 -> 40000,
        3 -> 60000,
        4 -> 100000,
        5 -> 200000,
        6 -> 500000,
        201 -> 20000,
        202 -> 50000,
        203 -> 100000,
        101-> 50000,
        102-> 200000,
        103-> 200000,
        104-> 20000,
        105-> 100000
      )

      val vnd = convertMap(amt.toInt)
      vnd.toString
    }
    }

    val setUserId = udf { (id: String, server:String) => {
      val userId = id+"_"+server
      userId
    }
    }

    if (hourly == "") {
      val pattern = Constants.GAMELOG_DIR + "/ygh5/[yyyyMMdd]/Log/dailyzalo/money_tb_iap_[yyyyMMdd].csv"
      var paymentPath: Array[String] = null
      if(logDate.contains("2017-12-22")) {
        paymentPath = PathUtils.generateLogPathDaily(pattern, logDate,1)
      }else{
        paymentPath = PathUtils.generateLogPathDaily(pattern, logDate)
      }

      paymentDs = getCsvLog(paymentPath, ",")

      val patternlLoginRegister = Constants.GAMELOG_DIR + "/ygh5/[yyyyMMdd]/Log/dailyzalo/*/*_tb_user_[yyyyMMdd].csv"
      val registerPath = PathUtils.generateLogPathDaily(patternlLoginRegister, logDate,1)
      registerDs = getCsvLog(registerPath, ",")

    }

    paymentDs=paymentDs.select("_c1","_c2","_c3","_c9","_c7","_c5","_c8")

    registerDs=registerDs.select("_c3","_c0","_c2").where("_c2 != '10000' and _c6 is not null")


    var join = paymentDs.as('a).join(registerDs.as('b), paymentDs("_c2") === registerDs("_c0") && paymentDs("_c8") === registerDs("_c2"), "left_outer")
      .select("_c1","a._c3","_c9","_c7","_c5","_c8","b._c3")


    join = join.withColumn(sf.ID,col("b._c3"))
      .withColumnRenamed("_c8", sf.SID)
      .withColumn(sf.RID, col(sf.ID))
      .withColumnRenamed("_c1", sf.LOG_DATE)
      .withColumnRenamed("_c7", sf.CHANNEL)
      .withColumn(sf.TRANS_ID, col("a._c3"))
      .withColumn(sf.GAME_CODE,lit("ygzl"))
    join=join.where("log_date like '"+logDate+"%' and _c5 = '1' and sid != '1000'")
    join = join.drop("_c5")
    join=join.withColumn(sf.GROSS_AMT,setAmtWithRate(col("_c9")))
    join=join.withColumn(sf.NET_AMT,setAmtWithRate(col("_c9")))
    join = join.drop("_c9")
    join = join.dropDuplicates(sf.TRANS_ID)
    join = join.select(sf.ID,sf.SID,sf.RID,sf.LOG_DATE,sf.CHANNEL,sf.TRANS_ID,sf.GAME_CODE,sf.GROSS_AMT,sf.NET_AMT)

    join
  }
}
