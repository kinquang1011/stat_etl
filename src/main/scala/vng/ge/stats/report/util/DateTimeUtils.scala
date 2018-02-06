package vng.ge.stats.report.util

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate}

/**
  * Created by vinhdp on 1/11/17.
  */
object DateTimeUtils {
	
	/**
	  * Get start date of timing's range before "logDate"
	  *
	  * @param logDate
	  * @param timing
	  * @return
	  */
	def getTimingStartDate(logDate: String, timing: String): String = {
		
		var startDate = logDate
		var toDate = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		
		toDate = timing match {
			case Constants.Timing.A1 => {
				toDate.minusDays(0)
			}
			case Constants.Timing.A3 => {
				toDate.minusDays(2)
			}
			case Constants.Timing.A7 => {
				toDate.minusDays(6)
			}
			case Constants.Timing.A14 => {
				toDate.minusDays(13)
			}
			case Constants.Timing.A30 => {
				toDate.minusDays(29)
			}
			case Constants.Timing.A60 => {
				toDate.minusDays(59)
			}
			case Constants.Timing.A90 => {
				toDate.minusDays(89)
			}
			case Constants.Timing.A180 => {
				toDate.minusDays(179)
			}
			case Constants.Timing.AC7 => {
				toDate.`with`(DayOfWeek.MONDAY)
			}
			case Constants.Timing.AC30 => {
				toDate.withDayOfMonth(1)
			}
			
			// for test only
			// vinhdp
			
			case Constants.Timing.A31 => {
				toDate.minusDays(30)
			}
		}
		
		startDate = toDate.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		startDate
	}
	
	def getPreviousDate(logDate: String, timing: String): String = {
		var startDate = logDate
		var toDate = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		
		toDate = timing match {
			case Constants.Timing.A1 => {
				toDate.minusDays(1)
			}
			case Constants.Timing.A3 => {
				toDate.minusDays(3)
			}
			case Constants.Timing.A7 => {
				toDate.minusDays(7)
			}
			case Constants.Timing.A14 => {
				toDate.minusDays(14)
			}
			case Constants.Timing.A30 => {
				toDate.minusDays(30)
			}
			case Constants.Timing.A60 => {
				toDate.minusDays(60)
			}
			case Constants.Timing.A90 => {
				toDate.minusDays(90)
			}
			case Constants.Timing.A180 => {
				toDate.minusDays(180)
			}
			case Constants.Timing.AC7 => {
				toDate.`with`(DayOfWeek.MONDAY)
			}
			case Constants.Timing.AC30 => {
				toDate.withDayOfMonth(1)
			}
		}
		
		startDate = toDate.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		startDate
	}
	
	def addDate(logDate: String, timing: String): String = {
		var startDate = logDate
		var toDate = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		
		toDate = timing match {
			case Constants.Timing.A1 => {
				toDate.plusDays(0)
			}
			case Constants.Timing.A3 => {
				toDate.plusDays(2)
			}
			case Constants.Timing.A7 => {
				toDate.plusDays(6)
			}
			case Constants.Timing.A14 => {
				toDate.plusDays(13)
			}
			case Constants.Timing.A30 => {
				toDate.plusDays(29)
			}
			case Constants.Timing.A60 => {
				toDate.plusDays(59)
			}
			case Constants.Timing.A90 => {
				toDate.plusDays(89)
			}
			case Constants.Timing.A180 => {
				toDate.plusDays(179)
			}
		}
		
		startDate = toDate.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		startDate
	}
	
	def isEndOfWeek(logDate: String): Boolean = {
		
		val date = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		if(date.getDayOfWeek.equals(DayOfWeek.SUNDAY)) {
			return true
		}
		false
	}
	
	def isEndOfMonth(logDate: String): Boolean = {
		
		val date = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		if(date.getDayOfMonth.equals(date.lengthOfMonth)) {
			return true
		}
		false
	}
	
	def getTimePeriod(logDate: String, timing: String): String = {
		
		var date = LocalDate.parse(logDate, DateTimeFormatter.ofPattern(Constants.Default.DATETIME_FORMAT))
		var result = ""
		
		timing match {
			case "a1" => {
				date = date
			}
			case "ac7" => {
				
				date = date.`with`(DayOfWeek.SUNDAY)
			}
			case "ac30" => {
				
				date = date.withDayOfMonth(date.lengthOfMonth)
			}
		}
		
		result = date.format(DateTimeFormatter.ofPattern(Constants.Default.DATE_FORMAT))
		result
	}
	
}
