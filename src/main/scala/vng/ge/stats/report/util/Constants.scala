package vng.ge.stats.report.util

/**
  * Created by vinhdp on 1/9/17.
  */
object Constants {
	
	val PREV = "prev"
	val NET_AMT = "net_amt"
	val GROSS_AMT = "gross_amt"
	val IN_GAME = "ingame"
	val SDK = "sdk"
	val PAYMENT = "payment"
	
	object Option {
		val PATH = "path"
		val MODE = "mode"
		val FORMAT = "format"
	}
	
	object GroupId {
		val SERVER = "sid"
		val CHANNEL = "channel"
		val PACKAGE = "package_name"
		val COUNTRY = "country_code"
		val OS = "os"
		val REVENUE_BY_USER = "revenue_by_user"
	}
	
	// global config
	object Default {
		
		val NAME_NODE = "hdfs://c408.hadoop.gda.lo:8020"
		val RAW_LOG_DIR = "/ge/gamelogs"
		val LOG_DIR = "/ge/warehouse"
		val FAIRY_LOG_DIR = "/ge/fairy/warehouse"
		val PARQUET_DIR = "/ge/warehouse"
		
		val MASTER = "yarn"
		val APP_NAME = "stats-report"
		val RUN_TYPE = "run"
		val RUN_TIMING = "a1,a3,a7,a14,a30,a60,ac7,ac30"
		val REPORT_NUMBER = "1-2-3-4-5-6-7-8-9-12"
		
		val CALC_ID = "id"
		val GROUP_ID = "sid"
		val SOURCE = "ingame"
		val TIMING = "a1"
		
		val TIME_ZONE = "GMT"
		val DATE_FORMAT = "yyyy-MM-dd"
		val DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
		
		val SEPARATOR = "/"
		val EMPTY_STRING = ""
		val FALSE_STRING = "false"
		val TRUE_STRING = "true"
		val GAME = "game"
	}
	
	object DataSources {
		val PARQUET = "parquet"
		val CSV = "csv"
		val ORC = "orc"
		val JDBC = "jdbc"
		val HCATALOG = "hcatalog"
	}
	
	object Parameters {
		val GAME_CODE = "game_code"
		val LOG_DATE = "log_date"
		val TIMING = "timing"
		val CALC_ID = "calc_id"
		val SOURCE = "source"
		val GROUP_ID = "group_id"
		
		// job coordinator params
		val JOB_NAME = "job_name"
		val RUN_TYPE = "run_type"
		val RUN_TIMING = "run_timing"
		val REPORT_NUMBER = "report_number"
		
		val HOURLY_REPORT = "hourly_report"
		val LOG_DIR = "log_dir"
	}
	
	object LogTypes {
		val ACTIVITY = "activity"
		val PAYMENT = "payment"
		val ACC_REGISTER = "acc_register"
		val FIRST_CHARGE = "first_charge"
		val CCU = "ccu"
		
		val TOTAL_LOGIN_ACC = "total_login_acc"
		val TOTAL_PAID_ACC = "total_paid_acc"
		
		val GROUP_MAPPING_FILE = "group_mapping_file"
		
		object Marketing {
			val APPS_FLYER = "appsflyer"
			val APPS_FLYER_TOTAL = "appsflyer_total"
		}

		val REVENUE_BY_USER = "revenue_by_user"
	}
	
	object LogNames {
		val ACTIVITY = "activity_2"
		val PAYMENT = "payment_2"
		val ACC_REGISTER = "accregister_2"
		val FIRST_CHARGE = "first_charge_2"
		val CCU = "ccu_2"
		
		val TOTAL_LOGIN_ACC = "total_login_acc_2"
		val TOTAL_PAID_ACC = "total_paid_acc_2"
		val REVENUE_BY_USER = "revenue_by_user"
	}
	
	object Kpi {
		
		// USER
		val ACTIVE = "active"
		val ACCOUNT_REGISTER = "account_register"
		val NEW_USER_RETENTION_RATE = "new_user_retention_rate"
		val USER_RETENTION_RATE = "user_retention_rate"
		
		val NEW_PLAYING = "new_playing"
		val NEW_ACCOUNT_PLAYING = "new_account_playing"
		val SERVER_NEW_ACCOUNT_PLAYING = "server_new_account_playing"
		val NEW_ROLE_PLAYING = "new_role_playing"
		val RETENTION_PLAYING = "retention_playing"
		val CHURN_PLAYING = "churn_playing"
		val NEW_USER_RETENTION = "new_user_retention"
		
		val RETENTION_PLAYING_RATE = "retention_playing_rate"
		val RETENTION_PAYING_RATE = "retention_paying_rate"
		val CHURN_PLAYING_RATE = "churn_playing_rate"
		
		// REVENUE
		val PAYING_USER = "paying"
		val NET_REVENUE = "net_revenue"
		val GROSS_REVENUE = "gross_revenue"
		val RETENTION_PAYING = "retention_paying"
		val CHURN_PAYING = "churn_paying"
		val NEW_PAYING = "new_paying"
		val NEW_PAYING_NET_REVENUE = "new_paying_net_revenue"
		val NEW_PAYING_GROSS_REVENUE = "new_paying_gross_revenue"
		val NEW_USER_PAYING = "new_user_paying"
		val NEW_USER_PAYING_NET_REVENUE = "new_user_paying_net_revenue"
		val NEW_USER_PAYING_GROSS_REVENUE = "new_user_paying_gross_revenue"
		
		val PLAYING_TIME = "playing_time"
		val AVG_PLAYING_TIME = "avg_playing_time"
		val CONVERSION_RATE = "conversion_rate"
		val ARRPU = "arrpu"
		val ARRPPU = "arrppu"
		
		// TRAFFIC
		val ACU = "acu"
		val PCU = "pcu"
		val NRU = "nru"
		val TOTAL_NRU = "total_nru"
		val NGR = "ngr"
		val CR = "cr"
		val RR = "rr"
		
		// USER RETENTION
		val USER_RETENTION = "user_retention"
	}
	
	object MarketingKpi {
		val INSTALL = "install"
		val NRU0 = "nru0"
        val NRU = "nru"
		val PU = "paying_user"
		val REV = "revenue"
	}
	
	object Timing {
		val HOURLY = "hourly"
		val A1 = "a1"
		val A7 = "a7"
		val A30 = "a30"
		val A60 = "a60"
		val A90 = "a90"
		val A180 = "a180"
		
		val A31 = "a31"
		
		val AC1 = "ac1"
		val AC7 = "ac7"
		val AC30 = "ac30"
		val AC60 = "ac60"
		
		val A2 = "a2"
		val A3 = "a3"
		val A14 = "a14"
	}
	
	object ReportNumber {
		
		val CCU                     = "1"
		val ACCOUNT_REGISTER        = "2"
		val ACTIVE_USER             = "3"
		val USER_RETENTION          = "4"
		val NEWUSER_RETENTION       = "5"
		val NEWUSER_REVENUE         = "6"
		val REVENUE                 = "7"
		val FIRST_CHARGE            = "8"
		val FIRST_CHARGE_RETENTION  = "9"
		
		val ROLE_REGISTER           = "10"
		val SERVER_ACCOUNT_REGISTER = "11"
		
		val PLAYING_TIME            = "12"
		val GAME_RETENTION          = "13"
		val TOTAL_NRU          = "14"
	}
}
