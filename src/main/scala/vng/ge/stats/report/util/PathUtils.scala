package vng.ge.stats.report.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
  * Created by vinhdp on 1/11/17.
  */
object PathUtils {
	
	def getFiles(path: String, logDate: String, timing: String): List[String] = {
		
		var files = List[String]()
		val startDate = DateTimeUtils.getTimingStartDate(logDate, timing)
		var date = ""
		
		var start = LocalDate.parse(startDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT, Locale.UK))
		val end = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT, Locale.UK))
		
		while(!start.isAfter(end)) {
			
			date = start.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
			files = s"$path/$date" :: files
			start = start.plusDays(1)
		}
		
		files
	}
	
	def getFile(path: String, logDate: String, timing: String): String = {
		
		val startDate = DateTimeUtils.getTimingStartDate(logDate, timing)
		s"$path/$startDate"
	}
	
	def makeFilePath(gameCode: String, fileName: String, isSDK: Boolean = false,
	                 isHourly: String, logDir: String): String = {
		
		var ubFolder = "ub/data"
		
		if(isSDK){
		
			ubFolder = "ub/sdk_data"
		}
		
		if(isHourly == Constants.Default.TRUE_STRING){
			
			ubFolder += "_hourly"
		}
		
		logDir + "/" + gameCode + "/" + ubFolder + "/" + fileName
	}
	
	def makePath(elements: String*): String = {
		
		val buffer = new StringBuilder
		
		elements.foreach(e => {
			buffer ++= e
			buffer ++= "/"
		})
		buffer.dropRight(1).toString()
	}
	
	def getFilePatterns(path: String, logDate: String, timing: String): String = {
		
		var pattern = ""
		val startDate = DateTimeUtils.getTimingStartDate(logDate, timing)
		var date = ""
		
		var start = LocalDate.parse(startDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT, Locale.UK))
		val end = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT, Locale.UK))
		
		while(!start.isAfter(end)) {
			
			date = start.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
			pattern +=  date + ","
			start = start.plusDays(1)
		}
		pattern = pattern.dropRight(1)
		
		s"$path${Constants.Default.SEPARATOR}{$pattern}"
	}
}
