package vng.ge.stats.etl.transform.adapter.sdk

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.SdkFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils

/**
  * Created by quangctn on 06/02/2017.
  */
class Stct extends SdkFormatter("stct") {

  import sqlContext.implicits._

  def start(args: Array[String]): Unit = {
    initParameters(args)
    setWarehouseDir(Constants.WAREHOUSE_DIR);
    sdkGameCode="STCT"
    gameCode="stct";
    timezone="0";
    sdkSource="sdk";
    changeRate=1;
    this -> run -> close
  }

}
