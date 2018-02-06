package vng.ge.stats.etl.transform.verify

import java.util.Calendar

import vng.ge.stats.etl.db.MysqlDB


/**
 * Created by tuonglv on 03/01/2017.
 */
class Verify {
    val FW_MONITOR = "fw_monitor"
    def insertMonitorLog(gameCode: String, monitorCode: String, message: String, level: String = "warning"): Unit = {
        val logDate = ""
        val now = Calendar.getInstance().getTime()
        val log_date_format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val messageDate = log_date_format.format(now)
        val sql = "insert into " + FW_MONITOR + " (game_code, monitor_code, level, monitor_message, log_date, message_date) values(" +
            "'" + gameCode + "','" + monitorCode + "','" + level + "','" + message + "','" + logDate + "','" + messageDate + "')"
        val mysqlDB = new MysqlDB()
        mysqlDB.executeUpdate(sql, thenClose = true) //execute sql and then close
        mysqlDB.close()
    }
}