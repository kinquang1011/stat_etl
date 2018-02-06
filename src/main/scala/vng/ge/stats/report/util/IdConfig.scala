package vng.ge.stats.report.util

/**
  * Created by vinhdp on 1/11/17.
  */
object IdConfig {
	val Ids = Map(
		
		/** USER KPI ID **/
		Constants.Kpi.ACTIVE                     -> 10000,
		Constants.Kpi.ACCOUNT_REGISTER           -> 11000,
		Constants.Kpi.NEW_ROLE_PLAYING           -> 12000,
		Constants.Kpi.RETENTION_PLAYING          -> 13000,
		Constants.Kpi.CHURN_PLAYING              -> 14000,
		
		Constants.Kpi.PAYING_USER                -> 15000,
		Constants.Kpi.NET_REVENUE              -> 16000,
		Constants.Kpi.RETENTION_PAYING           -> 17000,
		Constants.Kpi.CHURN_PAYING               -> 18000,
		Constants.Kpi.NEW_PAYING                 -> 19000,
		Constants.Kpi.NEW_PAYING_NET_REVENUE         -> 20000,
		
		Constants.Kpi.NRU                        -> 21000,
		Constants.Kpi.NGR                        -> 22000,
		Constants.Kpi.RR                         -> 23000,
		Constants.Kpi.CR                         -> 24000,
		
		Constants.Kpi.NEW_USER_PAYING            -> 25000,
		Constants.Kpi.NEW_USER_PAYING_NET_REVENUE    -> 26000,
		
		Constants.Kpi.NEW_USER_RETENTION         -> 27000,
		Constants.Kpi.NEW_USER_RETENTION_RATE    -> 28000,
		Constants.Kpi.USER_RETENTION_RATE        -> 29000,
		
		Constants.Kpi.ACU                        -> 30000,
		Constants.Kpi.PCU                        -> 31000,
		
		Constants.Kpi.RETENTION_PAYING_RATE      -> 32000,
		Constants.Kpi.SERVER_NEW_ACCOUNT_PLAYING -> 33000,
		
		Constants.Kpi.PLAYING_TIME               -> 34000,
		Constants.Kpi.USER_RETENTION             -> 35000,
		Constants.Kpi.AVG_PLAYING_TIME           -> 36000,
		Constants.Kpi.CONVERSION_RATE            -> 37000,
		Constants.Kpi.ARRPU                      -> 38000,
		Constants.Kpi.ARRPPU                     -> 39000,
		// vinhdp - 2017-02-13
		Constants.Kpi.GROSS_REVENUE              -> 52000,
		Constants.Kpi.NEW_PAYING_GROSS_REVENUE   -> 53000,
		Constants.Kpi.NEW_USER_PAYING_GROSS_REVENUE-> 54000,
		
		Constants.MarketingKpi.PU               -> 56000,
		Constants.MarketingKpi.REV              -> 57000,
		Constants.MarketingKpi.INSTALL          -> 58000,
		Constants.MarketingKpi.NRU0             -> 59000,
		Constants.MarketingKpi.NRU             	-> 60000,
		Constants.Kpi.TOTAL_NRU             	-> 61000
	)
	
	val Timings = Map(
		Constants.Timing.HOURLY     -> 0,
		Constants.Timing.A1     -> 1,
		Constants.Timing.A7     -> 7,
		Constants.Timing.A30    -> 30,
		Constants.Timing.A60    -> 60,
		Constants.Timing.A90    -> 90,
		Constants.Timing.A180    -> 180,
		
		Constants.Timing.AC1   -> 11,
		Constants.Timing.AC7   -> 17,
		Constants.Timing.AC30   -> 31,
		Constants.Timing.AC60   -> 61,
		
		Constants.Timing.A2     -> 2,
		Constants.Timing.A3     -> 3,
		Constants.Timing.A14    -> 14
	)
	
	def getKpiId(calcId: String, key: String, timing: String): Integer = {
		
		var kpiId = 0
		kpiId  = Ids.apply(key) + Timings.apply(timing)
		kpiId
	}
}
