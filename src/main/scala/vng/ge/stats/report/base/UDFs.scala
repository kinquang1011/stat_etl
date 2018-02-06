package vng.ge.stats.report.base

import org.apache.spark.sql.functions._
import vng.ge.stats.report.util.DateTimeUtils

/**
  * Created by vinhdp on 1/11/17.
  */
object UDFs {
	
	val makeOtherIfNull = udf {(str: String) => {
			
			if (str == null || str == "") "other" else str
		}
	}
	
	val makeOthersIfNull = udf {(str: String) => {
		
			if (str == null || str == "") "others" else str
		}
	}
	
	val getYYYYMMDD = udf {(logDate: String) => {
			logDate.substring(0, 10)
		}
	}
	
	val getTimePeriod = udf { (logDate: String, timing: String) =>
		DateTimeUtils.getTimePeriod(logDate, timing)
	}
	
	val changeGroup = udf { (groupId: String, gameCode: String) =>
		
		"vinhdp"
	}
}
