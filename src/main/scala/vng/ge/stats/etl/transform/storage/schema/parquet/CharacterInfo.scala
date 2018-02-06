package vng.ge.stats.etl.transform.storage.schema.parquet

import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.schema.Schema

/**
 * Created by tuonglv on 10/01/2017.
 */
class CharacterInfo  extends Schema {
    var schema: List[List[Any]] = List(
        List(Constants.FIELD_NAME.GAME_CODE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LOG_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.SID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.ID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.RID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.ROLE_NAME, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LEVEL, Constants.DATA_TYPE.INTEGER, Constants.ENUM0),
        List(Constants.FIELD_NAME.ONLINE_TIME, Constants.DATA_TYPE.LONG, Constants.ENUM0),
        List(Constants.FIELD_NAME.LAST_LOGIN_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.REGISTER_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.FIRST_CHARGE_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LAST_CHARGE_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.TOTAL_CHARGE, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
        List(Constants.FIELD_NAME.MONEY_BALANCE, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
        List(Constants.FIELD_NAME.FIRST_LOGIN_CHANNEL, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.FIRST_CHARGE_CHANNEL, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.MORE_INFO, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.RESOLUTION, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.NETWORK, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.CARRIER, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING)
    )
}