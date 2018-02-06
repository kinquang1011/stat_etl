package vng.ge.stats.etl.transform.verify

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.utils.{Common, SchemaUtil}

/**
 * Created by tuonglv on 03/01/2017.
 */
class CheckLogDuplicate extends Verify {

    def paymentDuplicateError(data: DataFrame, gameCode: String): Boolean = {
        var notDup: Boolean = false
        val limit = 10
        if(!SchemaUtil.getDFSchema(data).contains("net_amt")){
            return notDup
        }

        val checkDup = data.select("log_date", "id", "net_amt").sort("log_date", "id").limit(limit).collect().toArray
        val getSize = checkDup.length
        if (getSize == limit || (getSize != 0 && getSize % 2 == 0)) {
            var i = 0
            var needSame = true

            var continuesUser = true
            while (i < getSize - 1 && !notDup) {
                val t1 = checkDup(i)
                val t2 = checkDup(i + 1)
                if (t1(0) == t2(0) && t1(1) == t2(1) && t1(2) == t2(2)) {
                    needSame = false
                } else if (needSame) {
                    continuesUser = false
                    notDup = true
                } else {
                    continuesUser = false
                    needSame = true
                }
                i = i + 1
            }
            // check $getSize records dau` tien chi co 1 (user + log_date + net_amt)
            if (continuesUser) {
                val mess = "user_id = " + checkDup(0)(1) + ", log_date = " + checkDup(0)(0) + ", net_amt = " + checkDup(0)(2) + ", times = " + getSize
                Common.logger(mess)
                insertMonitorLog(gameCode, Constants.ERROR_CODE.PAYMENT_LOG_DUPLICATE, mess, Constants.ERROR_CODE.WARNING)
                notDup = true
            }

            if (!notDup) {
                val mess = "payment log duplicate, sample user_id is: " + checkDup(0)(1)
                Common.logger(mess)
                insertMonitorLog(gameCode, Constants.ERROR_CODE.PAYMENT_LOG_DUPLICATE, mess, Constants.ERROR_CODE.ERROR)
            }
        } else {
            notDup = true
        }
        val isDup = !notDup
        isDup
    }
}