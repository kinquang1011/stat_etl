package vng.ge.stats.etl.utils

/**
 * Created by tuonglv on 27/12/2016.
 */
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.text.ParseException
import java.util.Locale

import vng.ge.stats.etl.constant.Constants
object DateTimeUtils {

    /***
      * Get exactly the day before logDate by timing
      * vinhdp
      * 2016-07-05
      */
    def getDateBefore(logDate: String, timing: String): String = {

        var calcMethod = resolveCalculateMethod(timing)

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(date);

        var result = ""

        calcMethod match {

            case Constants.TIMING => {

                timing match {

                    case Constants.Timing.A1 => {
                        calendar.add(Calendar.DATE, -1)
                    }
                    case Constants.Timing.A3 => {
                        calendar.add(Calendar.DATE, -3)
                    }
                    case Constants.Timing.A7 => {
                        calendar.add(Calendar.DATE, -7)
                    }
                    case Constants.Timing.A14 => {
                        calendar.add(Calendar.DATE, -14)
                    }
                    case Constants.Timing.A30 => {
                        calendar.add(Calendar.DATE, -30)
                    }
                    case Constants.Timing.A60 => {
                        calendar.add(Calendar.DATE, -60)
                    }
                    case Constants.Timing.A90 => {
                        calendar.add(Calendar.DATE, -90)
                    }
                    case Constants.Timing.A180 => {
                        calendar.add(Calendar.DATE, -180)
                    }
                }
            }

            case Constants.KPI => {

                timing match {

                    case Constants.Timing.AC1 => {
                        calendar.add(Calendar.DATE, -1)
                    }
                    case Constants.Timing.AC7 => {
                        calendar.add(Calendar.DATE, -7)
                    }
                    case Constants.Timing.AC30 => {
                        calendar.add(Calendar.MONTH, -1)
                    }
                    case Constants.Timing.AC60 => {
                        calendar.add(Calendar.MONTH, -2)
                    }
                }
            }
        }

        result = format.format(calendar.getTime)

        return result
    }

    /***
      * Return the day that different from logDate exactly distance days
      */
    def getDate(logDate: String, distance: Int): String = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(date);

        var result = ""
        calendar.add(Calendar.DATE, distance)
        result = format.format(calendar.getTime)

        return result
    }

    def getWeeklyDate(logDate: String): List[String] = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK)

        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
        var listDate = List(format.format(calendar.getTime))
        for (i <- 1 to 6) {

            calendar.add(Calendar.DATE, 1)
            listDate = listDate ::: List(format.format(calendar.getTime))
        }

        return listDate
    }

    def oneDayInDatetime(logDate:String, _outputFormat: String, minutesPlus: Int): List[String] = {
        val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val outputFormat = new SimpleDateFormat(_outputFormat)
        val date = inputFormat.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK)
        calendar.setTime(date)
        val times = ((24 * 60) / minutesPlus)
        calendar.add(Calendar.MINUTE, minutesPlus)
        var listDate = List(outputFormat.format(calendar.getTime))
        for (i <- 1 to times - 1) {
            calendar.add(Calendar.MINUTE, minutesPlus)
            listDate = listDate ::: List(outputFormat.format(calendar.getTime))
        }
        return listDate
    }

    def getMonthlyDate(logDate: String): List[String] = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);

        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1)
        var listDate = List(format.format(calendar.getTime))
        var daysOfMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);

        for (i <- 1 to daysOfMonth - 1) {

            calendar.add(Calendar.DATE, 1)
            listDate = listDate ::: List(format.format(calendar.getTime))
        }

        return listDate
    }

    // vinhdp - 2016-06-09
    def get2MonthDate(logDate: String): List[String] = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);

        calendar.setTime(date);
        var listDate = List[String]()

        for(m <- 0 to 1){

            calendar.add(Calendar.MONTH, m * -1);
            calendar.set(Calendar.DAY_OF_MONTH, 1)


            listDate = listDate ::: List(format.format(calendar.getTime))
            var daysOfMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);

            for (i <- 1 to daysOfMonth - 1) {

                calendar.add(Calendar.DATE, 1)
                listDate = listDate ::: List(format.format(calendar.getTime))
            }
        }

        return listDate
    }

    def getListDateBefore(distance: Integer, logDate: String): List[String] = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(date);

        var listDate = List(format.format(calendar.getTime))
        for (i <- 1 to distance - 1) {

            calendar.add(Calendar.DATE, -1)
            listDate = listDate ::: List(format.format(calendar.getTime))
        }

        return listDate
    }

    def getDateDifferent(distance: Integer, logDate: String, calculatedBy: String, calculatedValue: String): String = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(date);

        var result = ""

        calculatedBy match {

            case Constants.TIMING => {

                calculatedValue match {

                    case Constants.Timing.A1 => {
                        calendar.add(Calendar.DATE, distance)
                    }
                    case Constants.Timing.A7 => {
                        calendar.add(Calendar.DATE, 7 * distance)
                    }
                    case Constants.Timing.A30 => {
                        calendar.add(Calendar.DATE, 30 * distance)
                    }
                    case Constants.Timing.A60 => {
                        calendar.add(Calendar.DATE, 60 * distance)
                    }
                    case Constants.Timing.A90 => {
                        calendar.add(Calendar.DATE, 90 * distance)
                    }
                    case Constants.Timing.A180 => {
                        calendar.add(Calendar.DATE, 180 * distance)
                    }
                }
            }

            case Constants.KPI => {

                calculatedValue match {

                    case Constants.Timing.AC1 => {
                        calendar.add(Calendar.DATE, distance)
                    }
                    case Constants.Timing.AC7 => {
                        calendar.add(Calendar.DATE, 7 * distance)
                    }
                    case Constants.Timing.AC30 => {
                        calendar.add(Calendar.MONTH, distance)
                    }
                    case Constants.Timing.AC60 => {
                        calendar.add(Calendar.MONTH, 2 * distance)
                    }
                }
            }
        }

        result = format.format(calendar.getTime)

        return result
    }

    def getTimestamp(logDate: String): Long = {

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = format.parse(logDate)
        return date.getTime
    }

    def getDate(date: String): Date = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val d = format.parse(date)

        return d
    }

    def getDate(date: String, pattern: String): Date = {

        var dateFormat = "yyyy-MM-dd" // Constants.DEFAULT_DATE_FORMAT
        if (pattern != null && pattern.length() != 0) {

            dateFormat = pattern
        }
        val format = new SimpleDateFormat(dateFormat)
        val d = format.parse(date)

        return d
    }

    def getDate(timestamp: Long): String = {

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val calendar = Calendar.getInstance()
        calendar.setTimeInMillis(timestamp)
        val date = calendar.getTime
        val dateString = format.format(date)

        return dateString
    }

    def getDateString(date: Date): String = {

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateString = format.format(date)

        return dateString
    }

    def formatDate(fromF: String, toF: String, date: String): String = {
        var result = date
        if (fromF != toF) {
            try {
                val fromFormat = new SimpleDateFormat(fromF)
                val toFormat = new SimpleDateFormat(toF)

                val d = fromFormat.parse(date)
                result = toFormat.format(d)
            } catch {
                case e: ParseException => e.printStackTrace()
            }
        }
        result
    }

    /**
     * author: canhtq
     */

    def getLogDate(fullDate: String): String = {
        val dateString = formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", fullDate);
        return dateString
    }

    def getTransactionDate(transId: String): String = {

        val transDate = transId.take(8)
        return formatDate("yyyyMMdd", "yyyy-MM-dd", transDate)
    }

    def getFirstDate(timing: String, logDate: String): String = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        var date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);

        calendar.setTime(date);
        var result = ""

        timing match {
            case "daily" => result = logDate
            case "weekly" => {

                calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
                result = format.format(calendar.getTime)
            }
            case "monthly" => {

                calendar.set(Calendar.DATE, 1)
                result = format.format(calendar.getTime)
            }
        }

        result
    }

    def getTimePeriod(timing: String, logDate: String): String = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        var date = format.parse(logDate)
        val calendar = Calendar.getInstance(Locale.UK);

        calendar.setTime(date);
        var result = ""

        timing match {
            case "a1" => result = format.format(calendar.getTime)
            case "ac7" => {

                calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
                calendar.add(Calendar.DATE, 6);
                result = format.format(calendar.getTime)
            }
            case "ac30" => {

                calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
                result = format.format(calendar.getTime)
            }
        }

        result
    }

    /**
     * Resolve calculate value to calculate method
     */
    def resolveCalculateMethod(value: String): String = {

        var result = ""

        value match {
            case Constants.Timing.A1 => result = Constants.TIMING
            case Constants.Timing.A3 => result = Constants.TIMING
            case Constants.Timing.A7 => result = Constants.TIMING
            case Constants.Timing.A14 => result = Constants.TIMING
            case Constants.Timing.A30 => result = Constants.TIMING
            case Constants.Timing.A60 => result = Constants.TIMING
            case Constants.Timing.A90 => result = Constants.TIMING
            case Constants.Timing.A180 => result = Constants.TIMING
            case Constants.Timing.AC1 => result = Constants.KPI
            case Constants.Timing.AC7 => result = Constants.KPI
            case Constants.Timing.AC30 => result = Constants.KPI
            case Constants.Timing.AC60 => result = Constants.KPI
        }

        result
    }
    def getPrevDate(logDate: String): String = {
        val calendarLast = Calendar.getInstance(Locale.UK)
        val lastDate = DateTimeUtils.getDate(logDate)
        calendarLast.setTime(lastDate)
        calendarLast.add(Calendar.DATE, -1)
        val formatLast = new SimpleDateFormat("yyyy-MM-dd")
        val logDateLast = formatLast.format(calendarLast.getTime)
        logDateLast
    }

    def getListDate(fromDate: String, toDate: String): List[String] = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val date = format.parse(fromDate)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(date);
        var dateStr = format.format(calendar.getTime)

        var listDate = List(dateStr)

        while (dateStr != toDate) {

            calendar.add(Calendar.DATE, 1)
            dateStr = format.format(calendar.getTime)
            listDate = listDate ::: List(dateStr)
        }

        return listDate
    }

    def isEndOfMonth(date: String): Boolean = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val d = format.parse(date)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(d);
        calendar.set(Calendar.DATE, 1)

        var daysOfMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        calendar.add(Calendar.DATE, daysOfMonth - 1)
        //println(format.format(calendar.getTime))
        if(format.format(calendar.getTime) == date){

            return true
        }

        return false;
    }

    def isEndOfWeek(date: String): Boolean = {

        val format = new SimpleDateFormat("yyyy-MM-dd")
        val d = format.parse(date)
        val calendar = Calendar.getInstance(Locale.UK);
        calendar.setTime(d);
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
        var weekend = format.format(calendar.getTime)
        println(weekend)
        if(weekend == date){

            return true
        }

        return false;
    }

    def getCurrentDatetime(): String ={
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val calendar = Calendar.getInstance(Locale.UK)
        val currentDatetime:String = format.format(calendar.getTime)
        currentDatetime
    }
    def getCurrentDate(): String ={
        val format = new SimpleDateFormat("yyyy-MM-dd")
        val calendar = Calendar.getInstance(Locale.UK)
        val currentDatetime:String = format.format(calendar.getTime)
        currentDatetime
    }

    def main(args: Array[String]) {
        if(isEndOfWeek("2016-07-24")){
            println("WEEKEND")
        }

        /*if(isEndOfMonth("2015-02-28")){
            println("END")
        }else{
            println("NO")
        }*/
    }
}
