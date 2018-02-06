package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.transform.udf.MyUdf
import vng.ge.stats.etl.utils.PathUtils


/**
  * Tên game: Bomb Me
  * +Game Code: 297
  * +Thể loại game: shooting Casual
  * +Thị trường: Thái
  * +Time Zone: GMT+7
  * +Timeline: 5/1/2018
  * +Tỉ giá convert từ tiền thực sang tiền game: 1 Baht = 20 coin
  * + Game có phân chia server
  * + Tần suất transfer log: daily
  * + Các yêu cầu hỗ trợ data từ product: Update sau
  * + Danh sách user sử dụng tool: HuynhTQ, ThuPV, KimPH2, sarujc@vng.com.vn (GO Thái)
  * 1 Baht =  690.7
  * mrtg code : 297
  * Payment : char
  * ETL : open bundle
  * REPORT:
  * + ingame : game_kpi
  * + sdk : sdk_game_kpi
  * +     : sdk_channel_kpi
  * :       sdk_os_kpi
  * :       sdk_package_kpi_2
  * + ccu: ccu
  * +
  **/
class Gnmthai extends FairyFormatter("gnmthai") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    this -> run() -> close()
  }

  private val convertToDateTime = udf { (datetime: String) => {
    var time = ""
    if (datetime != null && datetime.matches("\\d+")) {
      time = MyUdf.timestampToDate(datetime.toLong * 1000)
    }
    time

  }
  }

  override def getActivityDs(logDate: String, hourly: String): DataFrame = {
    var df: DataFrame = null
    val field = Constants.FIELD_NAME
    if (hourly == "") {
      val _logDate = logDate.replace("-", "")
      val pattern = Constants.GAMELOG_DIR + s"/gnmthai/${_logDate}/datalog/datalog/*/bombmethai*log_login*"
      df = getCsvWithHeaderLog(Array(pattern), ",").selectExpr("rid", "sid", "uid as id", "create_time as log_date")
    }
    df = df.withColumn(field.LOG_DATE, convertToDateTime(col(field.LOG_DATE)))
    df = df.withColumn(field.GAME_CODE, lit("gnmthai"))
    /*  .where(s"log_date like '%$logDate%'")*/
    df
  }

  override def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    var df: DataFrame = null
    if (hourly == "") {

      val pattern = Constants.GAMELOG_DIR + s"/gnmthai/${logDate.replace("-", "")}/datalog/datalog/*/bombmethai*log_charge*.csv"
      val path = PathUtils.generateLogPathDaily(pattern, logDate)
      df = getCsvWithHeaderLog(path, ",").selectExpr("rid", "uid as id", "sid", "create_time as log_date", "money")
    }
    val field = Constants.FIELD_NAME
    df = df.withColumn(field.GAME_CODE, lit("gnmthai"))
      .withColumn(field.GROSS_AMT, col("money") / 20 * 690.7)
      .withColumn(field.NET_AMT, col(field.GROSS_AMT))
      .drop("money")
      .withColumn(field.LOG_DATE, convertToDateTime(col(field.LOG_DATE)))
    /*  .where(s"log_date like '%$logDate%'")*/
    df.dropDuplicates(field.ID,field.LOG_DATE)
  }


}

