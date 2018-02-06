package vng.ge.stats.etl.constant

/**
 * Created by tuonglv on 27/12/2016.
 */
object Constants {
    val GAME_LOG_DIR = "/ge/gamelogs"

    val TIMING = "timing"
    val A1 = "a1"
    val A7 = "a7"
    val A30 = "a30"
    val A60 = "a60"
    val AC30 = "monthly"
    val AC60 = "2monthly"

    val KPI = "kpi"
    val DAILY = "daily"
    val WEEKLY = "weekly"
    val MONTHLY = "monthly"

    val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
    val DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

    val GAMELOG_DIR = "/ge/gamelogs"
    val WAREHOUSE_DIR = "/ge/warehouse"
    val FAIRY_WAREHOUSE_DIR = "/ge/fairy/warehouse"
    val TMP_DIR = "/user/fairy/tmp/warehouse"

    object STORAGE_TYPE {
        val PARQUET = "parquet"
        val HCATALOG = "hcatalog"
    }

    object LOG_TYPE {
        val DEVICE = "device"
        val ACTIVITY = "activity"
        val ROLE_REGISTER = "role_register"
        val LOGIN_LOGOUT = "login_logout"
        val ACC_REGISTER = "acc_register"
        val ACC_FIRST_CHARGE = "acc_first_charge"
        val SERVER_ACC_REGISTER = "server_acc_register"
        val PAYMENT = "payment"
        val PAYMENT_RAW = "payment_raw"
        val TOTAL_ACC_LOGIN = "total_acc_login"
        val TOTAL_ACC_PAID = "total_acc_paid"
        val TOTAL_DEVICE_PAID = "total_device_paid"
        val TOTAL_DEVICE_LOGIN = "total_device_login"
        val CCU = "ccu"
        val MONEY_FLOW = "money_flow"
        val CHARACTER_INFO = "character_info"
        val COUNTRY_MAPPING = "country_mapping"
        val DEVICE_REGISTER = "device_register"
        val DEVICE_FIRST_CHARGE = "device_first_charge"
        val REVENUE_BY_USER = "revenue_by_user"
    }

    object DATA_TYPE {
        val STRING = "string"
        val INTEGER = "int"
        val LONG = "long"
        val DOUBLE = "double"
    }

    val EMPTY_STRING = "''"
    val ENUM0 = 0
    val ENUM1 = 1

    object FOLDER_NAME {
        val ACTIVITY = "activity_2"
        val LOGIN_LOGOUT = "activity_2"
        val DEVICE = "device"
        val CCU = "ccu_2"
        val ACC_REGISTER = "accregister_2"
        val SERVER_ACC_REGISTER = "server_accregister_2"
        val ROLE_REGISTER = "roleregister_2"
        val DEVICE_REGISTER = "device_register_2"
        val ACC_FIRST_CHARGE = "first_charge_2"
        val DEVICE_FIRST_CHARGE = "device_first_charge_2"
        val PAYMENT = "payment_2"
        val MONEY_FLOW = "money_flow_2"
        val CHARACTER_INFO = "character_info_2"
        val TOTAL_ACC_LOGIN = "total_login_acc_2"
        val TOTAL_DEVICE_LOGIN = "total_login_device_2"
        val TOTAL_ACC_PAID = "total_paid_acc_2"
        val TOTAL_DEVICE_PAID = "total_paid_device_2"
        val COUNTRY_MAPPING = "country_mapping_2"
        val REVENUE_BY_USER = "revenue_by_user"
        val PAYMENT_RAW = "payment_raw"
    }

    object FIELD_NAME {
        val GAME_CODE = "game_code"
        val LOG_DATE = "log_date"
        val SID = "sid"
        val ID = "id"
        val RID = "rid"
        val ROLE_NAME = "role_name"
        val LEVEL = "level"
        val TRANS_ID = "trans_id"
        val CHANNEL = "channel"
        val PAY_CHANNEL = "pay_channel"
        val GROSS_AMT = "gross_amt"
        val NET_AMT = "net_amt"
        val XU_INSTOCK = "xu_instock"
        val XU_SPENT = "xu_spent"
        val XU_TOPUP = "xu_topup"
        val FIRST_CHARGE = "first_charge"
        val IP = "ip"
        val DEVICE = "device"
        val DID = "did"
        val OS = "os"
        val OS_VERSION = "os_version"
        val PACKAGE_NAME = "package_name"
        val ACTION = "action"
        val ONLINE_TIME = "online_time"
        val CCU = "ccu"

        val LAST_LOGIN_DATE = "last_login_date"
        val REGISTER_DATE = "register_date"
        val FIRST_CHARGE_DATE = "first_charge_date"
        val LAST_CHARGE_DATE = "last_charge_date"
        val TOTAL_CHARGE = "total_charge"
        val MONEY_BALANCE = "money_balance"
        val FIRST_LOGIN_CHANNEL = "first_login_channel"
        val FIRST_CHARGE_CHANNEL = "first_charge_channel"
        val POWER_AMT = "power_amt"
        val MORE_INFO = "more_info"

        val ACTION_MONEY = "action_money"
        val MONEY_AFTER = "money_after"
        val MONEY_TYPE = "money_type"
        val REASON = "reason"
        val SUB_REASON = "sub_reason"
        val ADD_OR_REDUCE = "add_or_reduce"

        val LOG_TYPE = "log_type"
        val COUNTRY_CODE = "country_code"

        val RESOLUTION = "resolution"
        val NETWORK = "network"
        val CARRIER = "carrier"
    }

    object INPUT_FILE_TYPE {
        val TSV = "tsv"
        val PARQUET = "parquet"
        val HIVE = "hive"
        val JSON = "json"
        val RAW_TEXT = "raw_text"
    }

    object OUTPUT_FILE_TYPE {
        val PARQUET = "parquet"
        val TSV = "tsv"
    }

    object Timing {
        val A1 = "a1"
        val A7 = "a7"
        val A30 = "a30"
        val A60 = "a60"
        val A90 = "a90"
        val A180 = "a180"
        val AC1 = "ac1"
        val AC7 = "ac7"
        val AC30 = "ac30"
        val AC60 = "ac60"
        val A2 = "a2"
        val A3 = "a3"
        val A14 = "a14"
    }

    object ERROR_CODE {
        val PAYMENT_MOBILE_LOG_NULL = "pmmn"
        val PAYMENT_LOG_NULL = "pmn"
        val PAYMENT_LOG_APPEND = "pmap"
        val PAYMENT_LOG_DUPLICATE = "pmdup"

        val ACC_REGISTER_RESET = "accrr"
        val FIRST_CHARGE_RESET = "fcr"

        val DEVICE_REGISTER_RESET = "device-rr"
        val DEVICE_FIRST_CHARGE_RESET = "device-fcr"

        val ERROR = "error"
        val WARNING = "warning"
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
        val NGR = "ngr"
        val CR = "cr"
        val RR = "rr"

        // USER RETENTION
        val USER_RETENTION = "user_retention"
    }

}