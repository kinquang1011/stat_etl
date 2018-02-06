package vng.ge.stats.etl.transform.storage.schema.parquet

import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.schema.Schema

/**
 * Created by tuonglv on 10/01/2017.
 */
class MoneyFlow extends  Schema {
    var schema: List[List[Any]] = List(
        List(Constants.FIELD_NAME.GAME_CODE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LOG_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.SID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.ID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.RID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LEVEL, Constants.DATA_TYPE.INTEGER, Constants.ENUM0),
        List(Constants.FIELD_NAME.ACTION_MONEY, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
        List(Constants.FIELD_NAME.MONEY_AFTER, Constants.DATA_TYPE.DOUBLE, Constants.ENUM0),
        List(Constants.FIELD_NAME.MONEY_TYPE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.REASON, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.SUB_REASON, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.ADD_OR_REDUCE, Constants.DATA_TYPE.INTEGER, Constants.ENUM0)
    )
}