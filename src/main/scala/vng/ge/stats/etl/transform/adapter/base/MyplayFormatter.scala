package vng.ge.stats.etl.transform.adapter.base

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.utils.{Common, DateTimeUtils}

/**
  * Created by tuonglv on 21/02/2017.
  */
class MyplayFormatter(_gameCode:String) extends Formatter(_gameCode) {
    setWarehouseDir(Constants.FAIRY_WAREHOUSE_DIR)

    override def getIdFirstChargeDs(logDate: String, _paymentDs: DataFrame, _totalAccPaidDs: DataFrame): DataFrame = {
        Common.logger("Kin quang")
        val sf = Constants.FIELD_NAME
        val firstCharge = _paymentDs.where(sf.FIRST_CHARGE +"=='1'")
        firstCharge
    }
    def writeTotalLogin(logDate: String, accRegisterDs: DataFrame,folderName: String): Unit = {
        val sf =Constants.FIELD_NAME
        var total: DataFrame = null
        val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
        val inputPath = Constants.FAIRY_WAREHOUSE_DIR+s"/$folderName/ub/data/"+Constants.FOLDER_NAME.TOTAL_ACC_LOGIN +"/"+oneDayAgo
        Common.logger("Path Total Login : "+inputPath)
        val totalAccLoginDs =sparkSession.read.parquet(inputPath)
        if (totalAccLoginDs != null && accRegisterDs != null) {
            total = accRegisterDs.select(sf.GAME_CODE, sf.LOG_DATE, sf.ID).union(totalAccLoginDs.select(sf.GAME_CODE, sf.LOG_DATE, sf.ID))
        } else if (totalAccLoginDs != null) {
            total = totalAccLoginDs.select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
        } else if (accRegisterDs != null) {
            total = accRegisterDs.select(sf.GAME_CODE, sf.LOG_DATE, sf.ID)
        }
        val output = s"/ge/fairy/warehouse/$folderName/ub/data/total_login_acc_2/$logDate"
        storeInParquet(total, output)
    }
     def  getMyplayIdRegisterDs(logDate: String, _activityDs: DataFrame,folderName :String): DataFrame = {
        val sf =Constants.FIELD_NAME
        var activityDs = _activityDs
        var totalAccLoginDs: DataFrame = null
        if (activityDs == null) {
            activityDs = getActivityDs(logDate,"")
        }
        if (totalAccLoginDs == null) {
            val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
            val inputPath = Constants.FAIRY_WAREHOUSE_DIR+s"/$folderName/ub/data/"+Constants.FOLDER_NAME.TOTAL_ACC_LOGIN +"/"+oneDayAgo
            Common.logger("Path Total Login : "+inputPath)
            totalAccLoginDs = sparkSession.read.parquet(inputPath)
        }
        totalAccLoginDs = totalAccLoginDs.selectExpr(sf.ID +" as ttid")
        var accRegisterDs = activityDs.as('p)
          .join(totalAccLoginDs.as('t), activityDs("id") === totalAccLoginDs("ttid"), "left_outer")
          .where("ttid is null").dropDuplicates("id")
         accRegisterDs = accRegisterDs.select(sf.GAME_CODE,sf.LOG_DATE,sf.ID)
         writeTotalLogin(logDate,accRegisterDs,folderName)
         accRegisterDs
    }

  def getHiveRegisterDs(logDate: String): DataFrame = {
    val sf = Constants.FIELD_NAME
    val query = s"select 'zingplaythai_lieng',log_date,user_id as id from zingplaythai_lieng.register where ds='$logDate'" +
      s"union all select 'zingplaythai_poker', log_date, user_id as id from zingplaythai_poker.register where ds='$logDate'" +
      s"union all select 'zingplaythai_dummy', log_date, user_id as id from zingplaythai_dummy.register where ds='$logDate'" +
      s"union all select ' zpimgsn_binh', log_date, user_id as id from  zpimgsn_binh.register where ds='$logDate'" +
      s"union all select ' coccmsea', log_date, user_id as id from  coccmsea.register where ds='$logDate'" +
      s"union all select ' ctpsea', log_date, user_id as id from  ctpsea.register where ds='$logDate'" +
      s"union all select ' sfmigsn', log_date, account_name as id from  sfmigsn.register where ds='$logDate'"
    var registerDs = getHiveLog(query)
    registerDs = registerDs.withColumnRenamed("zingplaythai_lieng",sf.GAME_CODE)
    registerDs= registerDs.dropDuplicates(sf.ID)
    registerDs
  }

}
