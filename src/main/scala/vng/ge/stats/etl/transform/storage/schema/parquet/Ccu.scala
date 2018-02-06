package vng.ge.stats.etl.transform.storage.schema.parquet

import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.schema.Schema

/**
 * Created by tuonglv on 10/01/2017.
 */
class Ccu extends Schema {
    var schema: List[List[Any]] = List(
        List(Constants.FIELD_NAME.GAME_CODE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.LOG_DATE, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.SID, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.OS, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.CHANNEL, Constants.DATA_TYPE.STRING, Constants.EMPTY_STRING),
        List(Constants.FIELD_NAME.CCU, Constants.DATA_TYPE.LONG, Constants.ENUM0)
    )
}