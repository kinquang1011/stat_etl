package vng.ge.stats.etl.utils

import vng.ge.stats.etl.constant.Constants

/**
  * Created by tuonglv on 27/12/2016.
  */
object PathUtils {
  /**
    * Generate logPath daily
    * Get two days by default (numberOfDay=2)
    */
  def generateLogPathDaily(pattern: String, from: String, numberOfDay: Int = 2): Array[String] = {
    var arr: Array[String] = Array()
    var f = pattern.replace("[yyyy-MM-dd]", from)
    f = f.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", from))
    arr = arr ++ Array(f)
    for (i <- 1 until numberOfDay ) {
      val date = DateTimeUtils.getDateDifferent(i * -1, from, Constants.TIMING, Constants.A1)
      var ff = pattern.replace("[yyyy-MM-dd]", date)
      ff = ff.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", date))
      arr = arr ++ Array(ff)
    }
    arr
  }

  def generateLogPathDailyWithFormat(pattern: String, from: String, dateformat: String, numberOfDay: Int = 2): Array[String] = {
    var arr: Array[String] = Array()
    var f = pattern.replace("[" + dateformat + "]", DateTimeUtils.formatDate("yyyy-MM-dd", dateformat, from))
    arr = arr ++ Array(f)
    for (i <- 1 until numberOfDay) {
      val date = DateTimeUtils.getDateDifferent(i * -1, from, Constants.TIMING, Constants.A1)
      var ff = pattern.replace("[" + dateformat + "]", DateTimeUtils.formatDate("yyyy-MM-dd", dateformat, date))
      arr = arr ++ Array(ff)
    }
    arr
  }

  /**
    * Generate logPath houtly
    * Get two hours by default (numberOfHour=2)
    */
  def generateLogPathHourly(pattern: String, from: String): Array[String] = {
    var arr: Array[String] = Array()
    var f = pattern.replace("[yyyy-MM-dd]", from)
    f = f.replace("[yyyyMMdd]", DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", from))
    arr = arr ++ Array(f)
    arr
  }

  def getSourceFolderName(dataSource: String, hourly: String = ""): String = {
    var sourceFolderName = ""
    if (dataSource == "ingame") {
      if (hourly == "") {
        sourceFolderName = "data"
      } else {
        sourceFolderName = "data_hourly"
      }
    } else if (dataSource == "sdk") {
      if (hourly == "") {
        sourceFolderName = "sdk_data"
      } else {
        sourceFolderName = "sdk_data_hourly"
      }
    }
    sourceFolderName
  }

  def getDataFolderName(logType: String): String = {
    var folderName = ""
    logType match {
      case Constants.LOG_TYPE.ACTIVITY =>
        folderName = Constants.FOLDER_NAME.ACTIVITY
      case Constants.LOG_TYPE.PAYMENT =>
        folderName = Constants.FOLDER_NAME.PAYMENT
      case Constants.LOG_TYPE.CCU =>
        folderName = Constants.FOLDER_NAME.CCU
      case Constants.LOG_TYPE.ACC_FIRST_CHARGE =>
        folderName = Constants.FOLDER_NAME.ACC_FIRST_CHARGE
      case Constants.LOG_TYPE.ACC_REGISTER =>
        folderName = Constants.FOLDER_NAME.ACC_REGISTER
      case Constants.LOG_TYPE.TOTAL_ACC_LOGIN =>
        folderName = Constants.FOLDER_NAME.TOTAL_ACC_LOGIN
      case Constants.LOG_TYPE.TOTAL_ACC_PAID =>
        folderName = Constants.FOLDER_NAME.TOTAL_ACC_PAID

      case Constants.LOG_TYPE.DEVICE_FIRST_CHARGE =>
        folderName = Constants.FOLDER_NAME.DEVICE_FIRST_CHARGE
      case Constants.LOG_TYPE.DEVICE_REGISTER =>
        folderName = Constants.FOLDER_NAME.DEVICE_REGISTER
      case Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN =>
        folderName = Constants.FOLDER_NAME.TOTAL_DEVICE_LOGIN
      case Constants.LOG_TYPE.TOTAL_DEVICE_PAID =>
        folderName = Constants.FOLDER_NAME.TOTAL_DEVICE_PAID
      case Constants.LOG_TYPE.REVENUE_BY_USER =>
        folderName = Constants.FOLDER_NAME.REVENUE_BY_USER

    }
    folderName
  }

  /**
    * @return
    */
  def getParquetPath(gameCode: String, logDate: String, rootDir: String, sourceFolder: String, dataFolder: String): String = {
    val output = rootDir + "/" + gameCode + "/ub/" + sourceFolder + "/" + dataFolder + "/" + logDate
    output
  }
}