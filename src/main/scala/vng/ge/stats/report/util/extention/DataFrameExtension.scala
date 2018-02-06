package vng.ge.stats.report.util

import org.apache.spark.sql.DataFrame

/**
  * Created by vinhdp on 2/6/17.
  */
package object extention {
	
	implicit class DataFrameExtension(val df: DataFrame) {
		
		def filterBetween(field: String, from: String, to: String) = df.where(s"$field >= '$from' and log_date <= '$to'")
		
		def filterGTE(field: String, from: String) = df.where(s"$field >= '$from'")
		
		def filterLTE(field: String, from: String) = df.where(s"$field <= '$from'")
		
		def filterLogDate(logDate: String, timing: String) = {
			
		}
		
		def changeGroupId(configDF: DataFrame, joinFields: Seq[String], dropFields: Seq[String]) = {
			
			df.join(configDF, joinFields, "left_outer").drop(dropFields:_*)
		}
	}
}
