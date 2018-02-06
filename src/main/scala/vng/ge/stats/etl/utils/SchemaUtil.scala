package vng.ge.stats.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.schema.Schema

/**
  * Created by tuonglv on 15/02/2017.
  */
object SchemaUtil {
    def getSchema(logType: String): Schema = {
        var schema: Schema = null
        logType match {
            case Constants.LOG_TYPE.ACTIVITY =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.Activity
            case Constants.LOG_TYPE.ACC_REGISTER =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.AccRegister
            case Constants.LOG_TYPE.PAYMENT =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.Payment
            case Constants.LOG_TYPE.ACC_FIRST_CHARGE =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.AccFirstCharge
            case Constants.LOG_TYPE.TOTAL_ACC_LOGIN =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.TotalAccLogin
            case Constants.LOG_TYPE.TOTAL_ACC_PAID =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.TotalAccPaid
            case Constants.LOG_TYPE.CCU =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.Ccu

            case Constants.LOG_TYPE.DEVICE_REGISTER =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.DeviceRegister
            case Constants.LOG_TYPE.DEVICE_FIRST_CHARGE =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.DeviceFirstCharge
            case Constants.LOG_TYPE.TOTAL_DEVICE_LOGIN =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.TotalDeviceLogin
            case Constants.LOG_TYPE.TOTAL_DEVICE_PAID =>
                schema = new vng.ge.stats.etl.transform.storage.schema.parquet.TotalDevicePaid
        }
        schema
    }

    def getDFSchema(ds: DataFrame): Array[String] = {
        var fields: Array[String] = Array()
        ds.schema.foreach { col =>
            fields = fields ++ Array(col.name)
        }
        fields
    }

    def getAllStringListSchema(ds: DataFrame): List[List[Any]] = {
        val fields: Array[String] = getDFSchema(ds)
        val length = fields.length

        var schema: List[List[Any]] = List()
        for (i <- 0 until length) {
            val fieldName = fields(i)
            schema = schema ++ List(
                List(fieldName, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING)
            )
        }
        schema
    }

    def getDefaultDsSchema(ds:DataFrame): List[List[Any]] = {
        val mapDataType = Map(
            "StringType" -> Array(Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
            "IntegerType" -> Array(Constants.DATA_TYPE.INTEGER, Constants.ENUM0),
            "DoubleType" -> Array(Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
            "LongType" -> Array(Constants.DATA_TYPE.LONG, Constants.ENUM0)
        )

        var schema: List[List[Any]] = List()
        ds.schema.foreach { col =>
            val fieldName = col.name
            val dataType = col.dataType.toString
            var _dataType: String = Constants.DATA_TYPE.STRING
            var _defaultValue: Any = Constants.EMPTY_STRING

            if(mapDataType.contains(dataType)){
                val _t1 = mapDataType(dataType)
                _dataType = _t1(0).toString
                _defaultValue = _t1(1)
            }
            schema = schema ++ List(
                List(fieldName, _dataType, _defaultValue)
            )
        }
        schema
    }
    def convertToStructType(_schema:Schema): StructType = {
        var aF: Array[StructField] = Array()
        _schema.schema.foreach { list =>
            val fieldName: String = list.head.toString
            val fieldType = list(1)
            fieldType match {
                case Constants.DATA_TYPE.DOUBLE =>
                    aF = aF ++ Array(StructField(fieldName, DoubleType, nullable = true))
                case Constants.DATA_TYPE.LONG =>
                    aF = aF ++ Array(StructField(fieldName, LongType, nullable = true))
                case Constants.DATA_TYPE.INTEGER =>
                    aF = aF ++ Array(StructField(fieldName, IntegerType, nullable = true))
                case Constants.DATA_TYPE.STRING =>
                    aF = aF ++ Array(StructField(fieldName, StringType, nullable = true))
            }
        }
        val _struct = StructType(aF)
        _struct
    }
}
