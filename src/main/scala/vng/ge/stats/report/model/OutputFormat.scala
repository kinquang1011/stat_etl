package vng.ge.stats.report.model

/**
  * Created by vinhdp on 1/11/17.
  */
case class KpiFormat(source: String, gameCode: String, logDate: String, createDate: String, kpiId: Integer, value: Double)
case class KpiGroupFormat(source: String, gameCode: String, groupId: String, logDate: String, createDate: String, kpiId: Integer, value: Double)
case class KpiGameRetentionFormat(source: String, gameCode: String, logDate: String, createDate: String, kpiId: Integer, value: String)

case class JsonFormat(source: String, gameCode: String, logDate: String, createDate: String, kpiId: Integer, value: String)
case class MarketingFormat(source: String, gameCode: String, logDate: String, mediaSource: String, campaign: String, os: String, createDate: String, kpiId: Integer, value: Double)