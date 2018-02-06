package vng.ge.stats.etl.transform.adapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.Formatter
import vng.ge.stats.etl.utils.PathUtils

class Coccm extends Formatter ("coccmgsn") {
	
	import sqlContext.implicits._
	
    def start(args: Array[String]): Unit = {
        initParameters(args)
        this->getTownHallLevelDs(_logDate, _hourly)->createDoneFlag(_logDate, _hourly)->close()
    }
    
    def getTownHallLevelDsFile(logDate: String, hourly: String) = {
	
	    val gameCode = this.gameCode
	    var logoutRaw: RDD[String] = null
	    var loginRaw: RDD[String] = null
	    
        if (hourly == "") {
	        val loginPattern = Constants.WAREHOUSE_DIR + s"/$gameCode/login/[yyyy-MM-dd]/*"
	        val loginPath = PathUtils.generateLogPathDaily(loginPattern, logDate)
	        loginRaw = getRawLog(loginPath)
	
	        val logoutPattern = Constants.WAREHOUSE_DIR + s"/$gameCode/logout/[yyyy-MM-dd]/*"
	        val logoutPath = PathUtils.generateLogPathDaily(logoutPattern, logDate)
	        logoutRaw = getRawLog(logoutPath)
        }
	    
	    val loginRDD = loginRaw.map(_.split("\t")).map(row => (gameCode, row(0), row(5), row(23)))
	    val logoutRDD = logoutRaw.map(_.split("\t")).map(row => (gameCode, row(0), row(5), row(23)))
	    
	    val logRDD = loginRDD.union(logoutRDD)
	    val df = logRDD.toDF("game_code", "log_date", "id", "hall_level")
	    val result = df.orderBy(asc("id"), desc("log_date")).dropDuplicates("id")
	    
	    val path = PathUtils.getParquetPath(gameCode, logDate, Constants.WAREHOUSE_DIR, "data", "hall_level")
        storeInParquet(result, path)
    }
	
	def getTownHallLevelDs(logDate: String, hourly: String) = {
		
		val gameCode = this.gameCode
		var loginDF: DataFrame = null
		var logoutDF: DataFrame = null
		
		if (hourly == "") {
			
			val loginQuery = s"select '$gameCode' as game_code, log_date, user_id as id, town_hall_level as group from coccm.login where ds ='$logDate'"
			loginDF = getHiveLog(loginQuery)
			
			val logoutQuery = s"select '$gameCode' as game_code, log_date, user_id as id, town_hall_level as group from coccm.logout where ds ='$logDate'"
			logoutDF = getHiveLog(logoutQuery)
		}
		
		val logDF = loginDF.union(logoutDF)
		val result = logDF.orderBy(desc("id"), desc("log_date")).dropDuplicates("id")
		
		val path = PathUtils.getParquetPath(gameCode, logDate, Constants.WAREHOUSE_DIR, "data", "hall_level")
		storeInParquet(result, path)
	}
}