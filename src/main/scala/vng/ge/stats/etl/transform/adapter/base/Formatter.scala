package vng.ge.stats.etl.transform.adapter.base

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SparkSession, _}
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.ParquetStorage
import vng.ge.stats.etl.transform.storage.schema.Schema
import vng.ge.stats.etl.transform.verify.CheckLogDuplicate
import vng.ge.stats.etl.utils.{Common, DateTimeUtils, PathUtils, SchemaUtil}
import org.apache.spark.sql.functions.{input_file_name, udf}
import vng.ge.stats.etl.transform.SparkEnv
import vng.ge.stats.report.util.Constants.LogTypes

import scala.util.Try
/**
  * Created by tuonglv on 26/12/2016.
  */

class Formatter(_gameCode: String, _dataSource: String = "ingame") {
  var gameCode: String = _gameCode
  val dataSource: String = _dataSource
  val sparkContext: SparkContext = SparkEnv.getSparkContext
  sparkContext.getConf.setAppName(gameCode.toUpperCase + " Formatter")
  val sqlContext: SQLContext = SparkEnv.getSqlContext
  val sparkSession: SparkSession = SparkEnv.getSparkSession

  var mapParameters: Map[String, String] = Map()

  import sparkSession.implicits._

  protected var _logDate = ""
  protected var _logType = ""
  protected var _hourly = ""
  private var _warehouseRootDir = Constants.WAREHOUSE_DIR
  private var _storageType: Array[String] = Array(Constants.STORAGE_TYPE.PARQUET)
  private var _didFormatter = false

  val emptyDataFrame: DataFrame = sqlContext.createDataFrame(sparkContext.emptyRDD[Row],
    StructType(StructField("game_code", StringType, nullable = true)
      :: StructField("log_date", StringType, nullable = true)
      :: StructField("id", StringType, nullable = true)
      //:: StructField("net_amt", DoubleType, nullable = true)
      :: Nil))


  def createEmptyDataFrame(_schema: Schema): DataFrame = {
    val struct = SchemaUtil.convertToStructType(_schema)
    val edf: DataFrame = sqlContext.createDataFrame(sparkContext.emptyRDD[Row], struct)
    edf
  }
  def createEmptyPaymentDs(): DataFrame = {
    val _sch = SchemaUtil.getSchema(Constants.LOG_TYPE.PAYMENT)
    createEmptyDataFrame(_sch)
  }
  def createEmptyActivityDs(): DataFrame = {
    val _sch = SchemaUtil.getSchema(Constants.LOG_TYPE.ACTIVITY)
    createEmptyDataFrame(_sch)
  }
  def createEmptyCCUDs(): DataFrame = {
    val _sch = SchemaUtil.getSchema(Constants.LOG_TYPE.CCU)
    createEmptyDataFrame(_sch)
  }
  def setWarehouseDir(wareHouseDir: String): Unit = {
    _warehouseRootDir = wareHouseDir
  }

  def setStorageType(storageType: Array[String]): Unit = {
    _storageType = storageType
  }
  def enableDIDFormatter(): Unit = {
    _didFormatter = true
  }

  def initParameters(args: Array[String]): Unit = {
    for (x <- args) {
      val xx = x.split("=")
      mapParameters += (xx(0) -> xx(1))
    }
    if (mapParameters.contains("logDate")) {
      _logDate = mapParameters("logDate")
    }
    if (mapParameters.contains("hourly")) {
      _hourly = mapParameters("hourly")
    }
    if (mapParameters.contains("logType")) {
      _logType = mapParameters("logType")
    }
  }

  def run(): Unit = {
    if (_logDate != "") {
      runInOneDay(_logType, _logDate, _hourly)
    } else {
      if (mapParameters.contains("rerun") && mapParameters("rerun") == "1") {
        val startDate = mapParameters("startDate")
        val endDate = mapParameters("endDate")
        var processDate = startDate
        while (processDate != endDate) {
          runInOneDay(_logType, processDate, _hourly)
          processDate = DateTimeUtils.getDateDifferent(1, processDate, Constants.TIMING, Constants.A1)
        }
      }
    }
  }

  private def runInOneDay(logType: String, logDate: String, hourly: String): Unit = {
    if (logType == "") {
      formatRequireLog(logDate, hourly)
    } else {
      _run(logType, logDate, hourly)
    }
    otherETL(logDate, hourly, logType)
    createDoneFlag(logDate, hourly)
  }

  def formatRequireLog(logDate: String, hourly: String): Unit = {
    //check duplicate first
    val Verify = new CheckLogDuplicate
    val paymentDs: DataFrame = getPaymentDs(logDate, hourly)
    //val isDup = Verify.paymentDuplicateError(paymentDs, gameCode)
    //if (isDup) return

    //activity, accRegister, deviceRegister
    val activityDs: DataFrame = getActivityDs(logDate, hourly)
    activityDs.cache()

    store(activityDs, logDate, Constants.LOG_TYPE.ACTIVITY)

    val accRegisterDs: DataFrame = getIdRegisterDs(logDate, activityDs)
    accRegisterDs.cache()
    store(accRegisterDs, logDate, Constants.LOG_TYPE.ACC_REGISTER)

    if (hourly == "") {
      store(getTotalAccLoginDs(logDate, _accRegisterDs = accRegisterDs), logDate, Constants.LOG_TYPE.TOTAL_ACC_LOGIN)
      if (_didFormatter) {
        val deviceRegisterDs = getDeviceRegisterDs(logDate, activityDs)
        deviceRegisterDs.cache
        store(deviceRegisterDs, logDate, Constants.LOG_TYPE.DEVICE_REGISTER)
        store(getTotalDeviceLoginDs(logDate, _deviceRegisterDs = deviceRegisterDs), logDate, Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN)
        deviceRegisterDs.unpersist()
      }
    }
    accRegisterDs.unpersist()

    //payment, firstCharge, deviceFirstCharge
    val paymentMoreDs = getJoinInfo(paymentDs, activityDs)
    store(paymentMoreDs, logDate, Constants.LOG_TYPE.PAYMENT)
    activityDs.unpersist()

    val firstChargeDs: DataFrame = getIdFirstChargeDs(logDate, paymentMoreDs)
    firstChargeDs.cache()
    store(firstChargeDs, logDate, Constants.LOG_TYPE.ACC_FIRST_CHARGE)
    if (hourly == "") {
      store(getTotalAccPaidDs(logDate, _accFirstChargeDs = firstChargeDs), logDate, Constants.LOG_TYPE.TOTAL_ACC_PAID)
      if (_didFormatter) {
        val deviceFirstChargeDs = getDeviceFirstChargeDs(logDate, paymentMoreDs)
        deviceFirstChargeDs.cache
        store(deviceFirstChargeDs, logDate, Constants.LOG_TYPE.DEVICE_REGISTER)
        store(getTotalDevicePaidDs(logDate, _deviceFirstChargeDs = deviceFirstChargeDs), logDate, Constants.LOG_TYPE.TOTAL_DEVICE_PAID)
        deviceFirstChargeDs.unpersist()
      }
    }
    firstChargeDs.unpersist()
    paymentMoreDs.unpersist()
    if (gameCode.equalsIgnoreCase("dt2")){
      _run(Constants.LOG_TYPE.COUNTRY_MAPPING,logDate,"")
    }
    if (hourly == "") {
      val ccuDs: DataFrame = getCcuDs(logDate, hourly)
      store(ccuDs, logDate, Constants.LOG_TYPE.CCU)
    }
  }

  def otherETL(logDate: String, hourly: String, logType: String): Unit = {

  }

  def createDoneFlag(logDate: String, hourly: String = ""): Unit = {
    var path = _warehouseRootDir + "/" + gameCode + "/ub/" + getSourceFolderName(hourly) + "/done-flag/" + logDate
    if (hourly != "") {
      path = path + "_" + hourly
    }
    Common.touchFile(path, sparkContext)
  }

  def getJoinInfo(poorDs: DataFrame, richDs: DataFrame, fieldId: String = Constants.FIELD_NAME.ID): DataFrame = {
    val poorDsFields = getDFSchema(poorDs)
    val richDsFields = getDFSchema(richDs)
    var needJoinField: Array[String] = Array()
    richDsFields.foreach { field =>
      if (!poorDsFields.contains(field) && richDsFields.contains(field)) {
        needJoinField = needJoinField ++ Array(field)
      }
    }
    var richDsSorted: DataFrame = emptyDataFrame
    if (richDsFields.contains("os") && richDsFields.contains("rid")) {
      richDsSorted = richDs.sort(richDs(fieldId), richDs("rid"), richDs("os").desc).dropDuplicates(Seq(fieldId, "rid"))
    } else if (richDsFields.contains("os")) {
      richDsSorted = richDs.sort(richDs(fieldId), richDs("os").desc).dropDuplicates(Seq(fieldId))
    } else if (richDsFields.contains("rid")) {
      richDsSorted = richDs.sort(richDs(fieldId), richDs("rid")).dropDuplicates(Seq(fieldId, "rid"))
    } else {
      richDsSorted = richDs.sort(richDs(fieldId)).dropDuplicates(Seq(fieldId))
    }
    richDsFields.foreach { f =>
      if (f != fieldId && f != "rid" && !needJoinField.contains(f.toString))
        richDsSorted = richDsSorted.drop(f.toString)
    }
    richDsSorted = richDsSorted.withColumnRenamed(fieldId, "ttid").withColumnRenamed("rid", "ttrid")
    var join: DataFrame = emptyDataFrame
    if (poorDsFields.contains("rid") && getDFSchema(richDsSorted).contains("ttrid")) {
      join = poorDs.as('ta).join(richDsSorted.as('tb),
        poorDs(fieldId) === richDsSorted("ttid")
          && poorDs("rid") === richDsSorted("ttrid"), "left_outer").drop("ttid").drop("ttrid")
    } else {
      join = poorDs.as('ta).join(richDsSorted.as('tb),
        poorDs(fieldId) === richDsSorted("ttid"), "left_outer").drop("ttid")
    }
    join
  }

  private def _run(logType: String, logDate: String, hourly: String): Unit = {
    logType match {
      case Constants.LOG_TYPE.ACTIVITY =>
        val activityDs = getActivityDs(logDate, hourly)
        activityDs.cache()
        val accRegister = getIdRegisterDs(logDate, activityDs)
        accRegister.cache()
        store(activityDs, logDate, Constants.LOG_TYPE.ACTIVITY)
        store(accRegister, logDate, Constants.LOG_TYPE.ACC_FIRST_CHARGE)
        store(getTotalAccLoginDs(logDate, _accRegisterDs = accRegister), logDate, Constants.LOG_TYPE.TOTAL_ACC_LOGIN)
        if (_didFormatter) {
          val deviceRegister = getDeviceRegisterDs(logDate, _activityDs = activityDs)
          deviceRegister.cache()
          store(deviceRegister, logDate, Constants.LOG_TYPE.DEVICE_REGISTER)
          store(getTotalDeviceLoginDs(logDate, _deviceRegisterDs = deviceRegister), logDate, Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN)
          deviceRegister.unpersist()
        }
        activityDs.unpersist()
        accRegister.unpersist()

      case Constants.LOG_TYPE.PAYMENT =>
        val activityDs = getActivityDs(logDate, hourly)
        val paymentDs = getPaymentDs(logDate, hourly)
        val paymentMoreDs = getJoinInfo(paymentDs, activityDs)
        paymentMoreDs.cache()
        val firstChargeDs = getIdFirstChargeDs(logDate, paymentMoreDs)
        store(paymentMoreDs, logDate, Constants.LOG_TYPE.PAYMENT)
        firstChargeDs.cache()
        store(firstChargeDs, logDate, Constants.LOG_TYPE.ACC_FIRST_CHARGE)
        store(getTotalAccPaidDs(logDate, _accFirstChargeDs = firstChargeDs), logDate, Constants.LOG_TYPE.TOTAL_ACC_PAID)
        if (_didFormatter) {
          val deviceFirstCharge = getDeviceFirstChargeDs(logDate, _paymentDs = paymentMoreDs)
          deviceFirstCharge.cache()
          store(deviceFirstCharge, logDate, Constants.LOG_TYPE.DEVICE_FIRST_CHARGE)
          store(getTotalDevicePaidDs(logDate, _deviceFirstChargeDs = deviceFirstCharge), logDate, Constants.LOG_TYPE.TOTAL_DEVICE_PAID)
          deviceFirstCharge.unpersist()
        }
        paymentMoreDs.unpersist()
        firstChargeDs.unpersist()

      case Constants.LOG_TYPE.CCU =>
        store(getCcuDs(logDate, hourly), logDate, Constants.LOG_TYPE.CCU)

      case Constants.LOG_TYPE.ACC_FIRST_CHARGE =>
        val firstChargeDs = getIdFirstChargeDs(logDate)
        firstChargeDs.cache()
        store(firstChargeDs, logDate, Constants.LOG_TYPE.ACC_FIRST_CHARGE)
        store(getTotalAccPaidDs(logDate, _accFirstChargeDs = firstChargeDs), logDate, Constants.LOG_TYPE.TOTAL_ACC_PAID)
        firstChargeDs.unpersist()

      case Constants.LOG_TYPE.DEVICE_FIRST_CHARGE =>
        val firstChargeDs = getDeviceFirstChargeDs(logDate)
        firstChargeDs.cache()
        store(firstChargeDs, logDate, Constants.LOG_TYPE.DEVICE_FIRST_CHARGE)
        store(getTotalDevicePaidDs(logDate, _deviceFirstChargeDs = firstChargeDs), logDate, Constants.LOG_TYPE.TOTAL_DEVICE_PAID)
        firstChargeDs.unpersist()

      case Constants.LOG_TYPE.ACC_REGISTER =>
        val accRegister = getIdRegisterDs(logDate)
        accRegister.cache()
        store(accRegister, logDate, Constants.LOG_TYPE.ACC_REGISTER)
        store(getTotalAccLoginDs(logDate, _accRegisterDs = accRegister), logDate, Constants.LOG_TYPE.TOTAL_ACC_LOGIN)
        accRegister.unpersist()

      case Constants.LOG_TYPE.DEVICE_REGISTER =>
        val deviceRegister = getDeviceRegisterDs(logDate)
        deviceRegister.cache()
        store(deviceRegister, logDate, Constants.LOG_TYPE.DEVICE_REGISTER)
        store(getTotalDeviceLoginDs(logDate, _deviceRegisterDs = deviceRegister), logDate, Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN)
        deviceRegister.unpersist()
      case Constants.LOG_TYPE.COUNTRY_MAPPING =>
        val countryDf = getCountryDf(logDate)
        store(countryDf, logDate, Constants.LOG_TYPE.COUNTRY_MAPPING)
      case default =>
        Common.logger(logType + " not found")
    }
  }

  def getRawLog(pathList: Array[String], appendLogPath: Boolean = false, appendDelimiter: String = "\\|"): RDD[String] = {
    Common.logger("getRawLog, path = " + pathList.mkString(","))
    var rs: RDD[String] = null
    if (appendLogPath) {
      val t1 = sqlContext.read.text(pathList: _*)
      val t2: Dataset[String] = t1.select(input_file_name(), t1("value")).map { line => line(0) + appendDelimiter + line(1) }
      rs = t2.rdd
    } else {
      rs = sparkContext.textFile(pathList.mkString(","), 8)
    }
    rs
  }

  def getParquetLog(pathList: Array[String]): DataFrame = {
    Common.logger("getParquetLog, path = " + pathList.mkString(","))
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.parquet(pathList: _*)
    }
    rs
  }

  def getParquetLog(path: String): DataFrame = {
    Common.logger("getParquetLog, path = " + path)
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.parquet(path)
    }
    rs
  }

  def getHiveLog(sql: String): DataFrame = {
    Common.logger("getHiveLog, sql = " + sql)
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.sql(sql)
    }
    rs
  }

  def getJsonLog(path: String): DataFrame = {
    Common.logger("getJsonLog, path = " + path)
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.json(path)
    }
    rs
  }
  def getJsonLog(pathList: Array[String]): DataFrame = {
    Common.logger("getJsonLog, path = " + pathList.mkString(","))
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.json(pathList: _*)
    }
    rs
  }

  def getCsvLog(path: String, dilemiter: String): DataFrame = {
    Common.logger("getCsvLog, path = " + path)
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.option("delimiter", dilemiter).csv(path)
    }
    rs
  }
  def getCsvLog(pathList: Array[String], delimiter: String): DataFrame = {
    Common.logger("getCsvLog, path = " + pathList.mkString(","))
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.option("delimiter", delimiter).csv(pathList: _*)
    }
    rs
  }

  def getCsvWithHeaderLog(pathList: Array[String], delimiter: String): DataFrame = {
    Common.logger("getCsvLog, path = " + pathList.mkString(","))
    var rs: DataFrame = emptyDataFrame
    Try {
      rs = sparkSession.read.option("delimiter", delimiter).option("header", "true").csv(pathList: _*)
    }
    rs
  }

  def store(ds: DataFrame, logDate: String, logType: String): Unit = {
    _storageType.distinct.foreach { sType =>
      Common.logger("store in " + sType)
      sType match {
        case Constants.STORAGE_TYPE.PARQUET =>
          storeInParquet(ds, logDate, logType)
        case Constants.STORAGE_TYPE.HCATALOG =>
          storeInHCatalog(ds, logDate, logType)
        case default =>
          Common.logger(sType + " not found")
      }
    }
  }

  def storeInHCatalog(ds: DataFrame, logDate: String, logType: String): Unit = {

  }

  def storeInParquet(ds: DataFrame, logDate: String, logType: String): Unit = {
    val outputPath = getParquetPath(logType, logDate)
    val parquetStorage = new ParquetStorage(getSchema(logType), outputPath)
    Common.logger("store, path = " + outputPath)
    parquetStorage.store(ds, sparkContext, sqlContext)
  }

  def storeInParquet(ds: DataFrame, outputPath: String, schema: Schema = null): Unit = {
    Common.logger("store, path = " + outputPath)
    val parquetStorage = new ParquetStorage(schema, outputPath)
    parquetStorage.store(ds, sparkContext, sqlContext)
  }

  def storeInJson(ds: DataFrame, outputPath: String, logType: String): Unit = {
    //val jsonStorage = new JsonStorage()
    //jsonStorage.store(ds)
  }

  def getParquetPath(logType: String, logDate: String, hourly: String = _hourly): String = {
    var sourceFolder = getSourceFolderName(hourly)
    val dataFolder = getDataFolderName(logType)
    if (logType == Constants.LOG_TYPE.TOTAL_ACC_LOGIN || logType == Constants.LOG_TYPE.TOTAL_ACC_PAID) {
      sourceFolder = sourceFolder.replace("_hourly", "")
    }
    val output = PathUtils.getParquetPath(gameCode, logDate, _warehouseRootDir, sourceFolder, dataFolder)
    output
  }

  def getSourceFolderName(hourly: String): String = {
    val sourceFolderName = PathUtils.getSourceFolderName(dataSource, hourly)
    sourceFolderName
  }

  def getDataFolderName(logType: String): String = {
    val folderName = PathUtils.getDataFolderName(logType)
    folderName
  }

  def getSchema(logType: String): Schema = {
    val schema: Schema = SchemaUtil.getSchema(logType)
    schema
  }

  def close(): Unit = {
    SparkEnv.stop()
  }

  def getActivityDs(logDate: String, hourly: String): DataFrame = {
    emptyDataFrame
  }

  def getPaymentDs(logDate: String, hourly: String): DataFrame = {
    emptyDataFrame
  }

  def getCcuDs(logDate: String, hourly: String): DataFrame = {
    emptyDataFrame
  }

  def getCountryDf(logDate: String, activeDf: DataFrame = null): DataFrame = {
    emptyDataFrame
  }

  def getIdFirstChargeDs(logDate: String, _paymentDs: DataFrame = null, _totalAccPaidDs: DataFrame = null): DataFrame = {
    var paymentDs = _paymentDs
    var totalAccPaidDs = _totalAccPaidDs
    if (paymentDs == null) {
      paymentDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.PAYMENT, logDate)))
    }
    if (totalAccPaidDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalAccPaidDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_ACC_PAID, oneDayAgo)))
    }
    val idFirstChargeDs = getFirstDs(logDate, Constants.FIELD_NAME.ID, paymentDs, totalAccPaidDs)
    idFirstChargeDs
  }

  def getDeviceFirstChargeDs(logDate: String, _paymentDs: DataFrame = null, _totalDevicePaidDs: DataFrame = null): DataFrame = {
    var paymentDs = _paymentDs
    var totalDevicePaidDs = _totalDevicePaidDs
    if (paymentDs == null) {
      paymentDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.PAYMENT, logDate)))
    }
    if (totalDevicePaidDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalDevicePaidDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_DEVICE_PAID, oneDayAgo)))
    }
    val deviceFirstChargeDs = getFirstDs(logDate, Constants.FIELD_NAME.DID, paymentDs, totalDevicePaidDs)
    deviceFirstChargeDs
  }

  private def getFirstDs(logDate: String, fieldId: String, _toGetFirstDs: DataFrame = null, _snapShotDs: DataFrame = null): DataFrame = {
    var toGetFirstDs = _toGetFirstDs
    var snapShot = _snapShotDs
    val toGetFirstDsSchema = getDFSchema(toGetFirstDs)
    if (toGetFirstDsSchema.contains("os")) {
      toGetFirstDs = toGetFirstDs.sort(toGetFirstDs(fieldId), toGetFirstDs("os").desc, toGetFirstDs("log_date").asc).dropDuplicates(Seq("id"))
    } else {
      toGetFirstDs = toGetFirstDs.sort(toGetFirstDs(fieldId), toGetFirstDs("log_date").asc).dropDuplicates(Seq(fieldId))
    }

    var firstDs = toGetFirstDs
    if (snapShot != null) {
      snapShot = snapShot.selectExpr(fieldId + " as ttid")
      firstDs = toGetFirstDs.as('p).join(snapShot.as('t), toGetFirstDs(fieldId) === snapShot("ttid"), "left_outer").where("ttid is null").drop("ttid")
    }
    firstDs
  }

  def getIdRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var activityDs = _activityDs
    var totalAccLoginDs = _totalAccLoginDs
    if (activityDs == null) {
      activityDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.ACTIVITY, logDate)))
    }
    if (totalAccLoginDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalAccLoginDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_ACC_LOGIN, oneDayAgo)))
    }
    val accRegisterDs = getFirstDs(logDate, Constants.FIELD_NAME.ID, activityDs, totalAccLoginDs)
    accRegisterDs
  }

  def getDeviceRegisterDs(logDate: String, _activityDs: DataFrame = null, _totalDeviceLoginDs: DataFrame = null): DataFrame = {
    var activityDs = _activityDs
    var totalDeviceLoginDs = _totalDeviceLoginDs
    if (activityDs == null) {
      activityDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.ACTIVITY, logDate)))
    }
    if (totalDeviceLoginDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalDeviceLoginDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN, oneDayAgo)))
    }
    val deviceRegisterDs = getFirstDs(logDate, Constants.FIELD_NAME.DID, activityDs, totalDeviceLoginDs)
    deviceRegisterDs
  }

  def getRoleRegisterDs(logDate: String, _activityDs: DataFrame = null): DataFrame = {
    null
  }

  private def getTotalAccLoginDs(logDate: String, _accRegisterDs: DataFrame = null, _totalAccLoginDs: DataFrame = null): DataFrame = {
    var totalAccLoginDs = _totalAccLoginDs
    var accRegisterDs: DataFrame = _accRegisterDs

    if (totalAccLoginDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalAccLoginDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_ACC_LOGIN, oneDayAgo)))
    }
    if (accRegisterDs == null) {
      accRegisterDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.ACC_REGISTER, logDate)))
    }
    var total: DataFrame = null
    if (totalAccLoginDs != null && accRegisterDs != null) {
      total = accRegisterDs.select("game_code", "log_date", "id").unionAll(totalAccLoginDs.select("game_code", "log_date", "id"))
    } else if (totalAccLoginDs != null) {
      total = totalAccLoginDs.select("game_code", "log_date", "id")
    } else if (accRegisterDs != null) {
      total = accRegisterDs.select("game_code", "log_date", "id")
    }
    total
  }

  private def getTotalAccPaidDs(logDate: String, _accFirstChargeDs: DataFrame = null, _totalAccPaidDs: DataFrame = null): DataFrame = {
    var totalAccPaidDs = _totalAccPaidDs
    var accFirstChargeDs: DataFrame = _accFirstChargeDs

    if (totalAccPaidDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalAccPaidDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_ACC_PAID, oneDayAgo)))
    }
    if (accFirstChargeDs == null) {
      accFirstChargeDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.ACC_FIRST_CHARGE, logDate)))
    }
    var total: DataFrame = null
    if (totalAccPaidDs != null && accFirstChargeDs != null) {
      total = accFirstChargeDs.select("game_code", "log_date", "id").unionAll(totalAccPaidDs.select("game_code", "log_date", "id"))
    } else if (totalAccPaidDs != null) {
      total = totalAccPaidDs.select("game_code", "log_date", "id")
    } else if (accFirstChargeDs != null) {
      total = accFirstChargeDs.select("game_code", "log_date", "id")
    }
    total
  }

  private def getTotalDeviceLoginDs(logDate: String, _deviceRegisterDs: DataFrame = null, _totalDeviceLoginDs: DataFrame = null): DataFrame = {
    var totalDeviceLoginDs = _totalDeviceLoginDs
    var deviceRegisterDs: DataFrame = _deviceRegisterDs

    if (totalDeviceLoginDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalDeviceLoginDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN, oneDayAgo)))
    }
    if (deviceRegisterDs == null) {
      deviceRegisterDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.DEVICE_REGISTER, logDate)))
    }
    var total: DataFrame = null
    if (totalDeviceLoginDs != null && deviceRegisterDs != null) {
      total = deviceRegisterDs.select("game_code", "log_date", "did").unionAll(totalDeviceLoginDs.select("game_code", "log_date", "did"))
    } else if (totalDeviceLoginDs != null) {
      total = totalDeviceLoginDs.select("game_code", "log_date", "did")
    } else if (deviceRegisterDs != null) {
      total = deviceRegisterDs.select("game_code", "log_date", "did")
    }
    total
  }

  private def getTotalDevicePaidDs(logDate: String, _deviceFirstChargeDs: DataFrame = null, _totalDevicePaidDs: DataFrame = null): DataFrame = {
    var totalDevicePaidDs = _totalDevicePaidDs
    var deviceFirstChargeDs: DataFrame = _deviceFirstChargeDs

    if (totalDevicePaidDs == null) {
      val oneDayAgo = DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1)
      totalDevicePaidDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.TOTAL_DEVICE_PAID, oneDayAgo)))
    }
    if (deviceFirstChargeDs == null) {
      deviceFirstChargeDs = getParquetLog(Array(getParquetPath(Constants.LOG_TYPE.DEVICE_FIRST_CHARGE, logDate)))
    }
    var total: DataFrame = null
    if (totalDevicePaidDs != null && deviceFirstChargeDs != null) {
      total = deviceFirstChargeDs.select("game_code", "log_date", "did").unionAll(totalDevicePaidDs.select("game_code", "log_date", "did"))
    } else if (totalDevicePaidDs != null) {
      total = totalDevicePaidDs.select("game_code", "log_date", "did")
    } else if (deviceFirstChargeDs != null) {
      total = deviceFirstChargeDs.select("game_code", "log_date", "did")
    }
    total
  }

  def getDFSchema(ds: DataFrame): Array[String] = {
    SchemaUtil.getDFSchema(ds)
  }

}
