package vng.ge.stats.etl.transform.storage.schema.parquet

import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.schema.Schema

/**
 * Created by tuonglv on 09/01/2017.
 */
class Payment extends Schema {
    var schema: List[List[Any]] = List(
        List(Constants.FIELD_NAME.GAME_CODE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LOG_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.PACKAGE_NAME, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.SID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.ID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.RID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.ROLE_NAME, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LEVEL, Constants.DATA_TYPE.INTEGER, Constants.ENUM0),
        List(Constants.FIELD_NAME.TRANS_ID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.CHANNEL, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.PAY_CHANNEL, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.GROSS_AMT, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
        List(Constants.FIELD_NAME.NET_AMT, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
        List(Constants.FIELD_NAME.XU_INSTOCK, Constants.DATA_TYPE.LONG, Constants.ENUM0),
        List(Constants.FIELD_NAME.XU_SPENT, Constants.DATA_TYPE.LONG, Constants.ENUM0),
        List(Constants.FIELD_NAME.XU_TOPUP, Constants.DATA_TYPE.LONG, Constants.ENUM0),
        List(Constants.FIELD_NAME.IP, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.DEVICE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.OS, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.OS_VERSION, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.DID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.RESOLUTION, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.NETWORK, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.CARRIER, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.POWER_AMT, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0)
    )
}