package vng.ge.stats.report.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneId
/**
  * Created by vinhdp on 1/9/17.
  */
object Logger {
	
	private val signal = "STATS"
	
	def info(msg: String, signal: String = signal, tab: Int = 0): Unit = {
		val now = LocalDateTime.now(ZoneId.of("GMT+7"))
		val time = now.format(DateTimeFormatter.ofPattern(Constants.Default.DATETIME_FORMAT))
		println(signal + " INFO: " + time + " " + addTabBeforeMessage(tab, msg))
	}
	
	def error(msg: String, signal: String = signal, tab: Int = 0): Unit = {
		val now = LocalDateTime.now(ZoneId.of("GMT+7"))
		val time = now.format(DateTimeFormatter.ofPattern(Constants.Default.DATETIME_FORMAT))
		println(signal + " ERROR: " + time + " " + addTabBeforeMessage(tab, msg))
	}
	
	def warning(msg: String, signal: String = signal, tab: Int = 0): Unit = {
		val now = LocalDateTime.now(ZoneId.of("GMT+7"))
		val time = now.format(DateTimeFormatter.ofPattern(Constants.Default.DATETIME_FORMAT))
		println(signal + " WARN: " + time + " " + addTabBeforeMessage(tab, msg))
	}
	
	def addTabBeforeMessage(num: Int, msg: String): String = {
		
		var tab = ""
		
		if(num != 0) {
			tab = "|"
		}
		
		for(i <- 0 to num - 1){
			tab += "\t"
		}
		
		tab + msg
	}
}
