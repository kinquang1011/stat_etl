package vng.ge.stats.report.model

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{StructField, StructType, _}
import vng.ge.stats.report.util.Constants

/**
  * Created by vinhdp on 1/18/17.
  */
object Schemas {
	
	object FieldName {
		val GAME_CODE = "game_code"
		val LOG_DATE = "log_date"
		val PACKAGE_NAME = "package_name"
		val SID = "sid"
		val ID = "id"
		val RID = "rid"
		val ROLE_NAME = "role_name"
		val DID = "did"
		val ACTION = "action"
		val CHANNEL = "channel"
		val ONLINE_TIME = "online_time"
		val LEVEL = "level"
		val IP = "ip"
		val DEVICE = "device"
		val OS = "os"
		val OS_VERSION = "os_version"
		val RESOLUTION = "resolution"
		val NETWORK = "network"
		val CARRIER = "carrier"
		val TRANS_ID = "trans_id"
		val PAY_CHANNEL = "pay_channel"
		val GROSS_AMT = "gross_amt"
		val NET_AMT = "net_amt"
		val XU_INSTOCK = "xu_instock"
		val XU_SPENT = "xu_spent"
		val XU_TOPUP = "xu_topup"
		val POWER_AMT = "power_amt"
		val CCU = "ccu"
		
		val GROUP = "group"
		val REG_DATE = "reg_date"
		val REV1 = "rev1"
		val REV2 = "rev2"
		val REV3 = "rev3"
		val REV4 = "rev4"
		val REV5 = "rev5"
		val REV6 = "rev6"
		val REV7 = "rev7"
		val REV14 = "rev14"
		val REV28 = "rev28"
		val REV30 = "rev30"
		val REV42 = "rev42"
		val REV60 = "rev60"
		val REV_TOTAL = "rev_total"
	}
	
	val Activity = StructType(
			StructField(FieldName.GAME_CODE, StringType, true) ::
			StructField(FieldName.LOG_DATE, StringType, true) ::
			StructField(FieldName.PACKAGE_NAME, StringType, true) ::
			StructField(FieldName.SID, StringType, true) ::
			StructField(FieldName.ID, StringType, true) ::
			StructField(FieldName.RID, StringType, true) ::
			StructField(FieldName.ROLE_NAME, StringType, true) ::
			StructField(FieldName.DID, StringType, true) ::
			StructField(FieldName.ACTION, StringType, true) ::
			StructField(FieldName.CHANNEL, StringType, true) ::
			StructField(FieldName.ONLINE_TIME, LongType, true) ::
			StructField(FieldName.LEVEL, IntegerType, true) ::
			StructField(FieldName.IP, StringType, true) ::
			StructField(FieldName.DEVICE, StringType, true) ::
			StructField(FieldName.OS, StringType, true) ::
			StructField(FieldName.OS_VERSION, StringType, true) ::
			StructField(FieldName.RESOLUTION, StringType, true) ::
			StructField(FieldName.NETWORK, StringType, true) ::
			StructField(FieldName.CARRIER, StringType, true) :: Nil
	)
	
	val AccountRegister = StructType(
			StructField(FieldName.GAME_CODE,StringType,true) ::
			StructField(FieldName.LOG_DATE,StringType,true) ::
			StructField(FieldName.PACKAGE_NAME,StringType,true) ::
			StructField(FieldName.CHANNEL,StringType,true) ::
			StructField(FieldName.SID,StringType,true) ::
			StructField(FieldName.ID,StringType,true) ::
			StructField(FieldName.IP,StringType,true) ::
			StructField(FieldName.DEVICE,StringType,true) ::
			StructField(FieldName.OS,StringType,true) ::
			StructField(FieldName.OS_VERSION,StringType,true) ::
			StructField(FieldName.ROLE_NAME,StringType,true) ::
			StructField(FieldName.RESOLUTION,StringType,true) ::
			StructField(FieldName.CARRIER,StringType,true) :: Nil
	)
	
	val Payment = StructType(
			StructField(FieldName.GAME_CODE,StringType,true) ::
			StructField(FieldName.LOG_DATE,StringType,true) ::
			StructField(FieldName.PACKAGE_NAME,StringType,true) ::
			StructField(FieldName.SID,StringType,true) ::
			StructField(FieldName.ID,StringType,true) ::
			StructField(FieldName.RID,StringType,true) ::
			StructField(FieldName.ROLE_NAME,StringType,true) ::
			StructField(FieldName.LEVEL,IntegerType,true) ::
			StructField(FieldName.TRANS_ID,StringType,true) ::
			StructField(FieldName.CHANNEL,StringType,true) ::
			StructField(FieldName.PAY_CHANNEL,StringType,true) ::
			StructField(FieldName.GROSS_AMT,DoubleType,true) ::
			StructField(FieldName.NET_AMT,DoubleType,true) ::
			StructField(FieldName.XU_INSTOCK,LongType,true) ::
			StructField(FieldName.XU_SPENT,LongType,true) ::
			StructField(FieldName.XU_TOPUP,LongType,true) ::
			StructField(FieldName.IP,StringType,true) ::
			StructField(FieldName.DEVICE,StringType,true) ::
			StructField(FieldName.OS_VERSION,StringType,true) ::
			StructField(FieldName.OS_VERSION,StringType,true) ::
			StructField(FieldName.ROLE_NAME,StringType,true) ::
			StructField(FieldName.RESOLUTION,StringType,true) ::
			StructField(FieldName.NETWORK,StringType,true) ::
			StructField(FieldName.CARRIER,StringType,true) ::
			StructField(FieldName.POWER_AMT,DoubleType,true) ::Nil
	)
	
	val FirstCharge = StructType(
			StructField(FieldName.GAME_CODE,StringType,true) ::
			StructField(FieldName.LOG_DATE,StringType,true) ::
			StructField(FieldName.PACKAGE_NAME,StringType,true) ::
			StructField(FieldName.CHANNEL,StringType,true) ::
			StructField(FieldName.PAY_CHANNEL,StringType,true) ::
			StructField(FieldName.SID,StringType,true) ::
			StructField(FieldName.ID,StringType,true) ::
			StructField(FieldName.IP,StringType,true) ::
			StructField(FieldName.DEVICE,StringType,true) ::
			StructField(FieldName.OS,StringType,true) ::
			StructField(FieldName.OS_VERSION,StringType,true) ::
			StructField(FieldName.RESOLUTION,StringType,true) ::
			StructField(FieldName.NETWORK,StringType,true) ::
			StructField(FieldName.CARRIER,StringType,true) ::Nil
	)
	
	val Ccu = StructType(
			StructField(FieldName.GAME_CODE,StringType,true) ::
			StructField(FieldName.LOG_DATE,StringType,true) ::
			StructField(FieldName.SID,StringType,true) ::
			StructField(FieldName.OS,DoubleType,true) ::
			StructField(FieldName.CHANNEL,StringType,true) ::
			StructField(FieldName.CCU,LongType,true) :: Nil
	)
	
	val Mapping = StructType(
		StructField(FieldName.GAME_CODE,StringType,true) ::
		StructField(FieldName.LOG_DATE,StringType,true) ::
		StructField(FieldName.ID,StringType,true) ::
		StructField(FieldName.GROUP,StringType,true) :: Nil
	)
	
	val Unknow = StructType(
			StructField(FieldName.GAME_CODE,StringType,true) ::
			StructField(FieldName.ID,StringType,true) :: Nil
	)

	val RevenueByUser = StructType(
		StructField(FieldName.GAME_CODE,StringType,true) ::
		StructField(FieldName.REG_DATE,StringType,true) ::
		StructField(FieldName.ID,StringType,true) ::
		StructField(FieldName.REV1,DoubleType,true) ::
		StructField(FieldName.REV2,DoubleType,true) ::
		StructField(FieldName.REV3,DoubleType,true) ::
		StructField(FieldName.REV4,DoubleType,true) ::
		StructField(FieldName.REV5,DoubleType,true) ::
		StructField(FieldName.REV6,DoubleType,true) ::
		StructField(FieldName.REV7,DoubleType,true) ::
		StructField(FieldName.REV14,DoubleType,true) ::
		StructField(FieldName.REV28,DoubleType,true) ::
		StructField(FieldName.REV30,DoubleType,true) ::
		StructField(FieldName.REV42,DoubleType,true) ::
		StructField(FieldName.REV60, DoubleType,true) ::
		StructField(FieldName.REV_TOTAL,DoubleType,true) :: Nil
	)

	def getSchema(fileType: String, defaultSchema: StructType): StructType = {
		
		fileType match {
			case Constants.LogNames.CCU => Ccu
			case Constants.LogNames.ACTIVITY => Activity
			case Constants.LogNames.ACC_REGISTER => AccountRegister
			case Constants.LogNames.PAYMENT => Payment
			case Constants.LogNames.FIRST_CHARGE => FirstCharge
			case Constants.LogNames.REVENUE_BY_USER => RevenueByUser

			case _ => defaultSchema
		}
	}
	def getUserIDSchema(): StructType ={
		UserID
}
	val UserID = StructType(
			StructField("userID",StringType,true) :: Nil
	)
}
