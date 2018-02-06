package vng.ge.stats.report.sql.mysql

import vng.ge.stats.report.model.{JsonFormat, KpiFormat, MarketingFormat}
import vng.ge.stats.report.sql.DbMySql
import vng.ge.stats.report.util.Logger

import scala.collection.mutable.LinkedHashMap


object MarketingMysqlReport {
    
    var mysql: DbMySql = new DbMySql()
    
    def insertJson(lstOutput: List[JsonFormat]): Unit = {
        
        deleteJson(lstOutput)
        var results = List[LinkedHashMap[String, Any]]()
        implicit val formats = net.liftweb.json.DefaultFormats
        lstOutput.foreach(
            row => {
                var mp: LinkedHashMap[String, Any] = LinkedHashMap("logDate" -> row.logDate,
                    "gameCode" -> row.gameCode,
                    "source" -> row.source,
                    "kpiId" -> row.kpiId,
                    "value" -> row.value,
                    "createDate" -> row.createDate
                )
                results ++= List(mp)
            }
        )
        
        val sql = "insert into test_marketing_json (report_date, game_code, source, kpi_id, kpi_value, calc_date) values (?, ?, ?, ?, ?, ?)"
        //println(sql)
        mysql.excuteBatchInsert(sql, results)
    }
    
    def deleteJson(lstOutput: List[JsonFormat]): Unit = {

        var results = List[LinkedHashMap[String, Any]]()
        
        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("kpiId" -> values.kpiId)
            
            results ++= List(row)
        }
        
        val sql = "delete from test_marketing_json where source = ? and game_code = ? and report_date = ? and kpi_id = ?"
        //println(sql)
        mysql.excuteBatchDelete(sql, results)
    }
    
    def insert(lstOutput: List[MarketingFormat]): Unit = {
        
        delete(lstOutput)
        
        var results = List[LinkedHashMap[String, Any]]()
        
        for (values <- lstOutput) {
            
            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("os" -> values.os)
            row += ("mediaSource" -> values.mediaSource)
            row += ("campaign" -> values.campaign)
            row += ("createDate" -> values.createDate)
            row += ("kpiId" -> values.kpiId)
            row += ("value" -> values.value)
            
            results ++= List(row)
        }
        val sql = "insert into marketing_kpi (source, game_code, report_date, os, media_source, campaign, calc_date, kpi_id, kpi_value) values (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        mysql.excuteBatchInsert(sql, results)
    }
    
    def delete(lstOutput: List[MarketingFormat]): Unit = {
        
        var results = List[LinkedHashMap[String, Any]]()
        
        for (values <- lstOutput) {
            
            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("kpiId" -> values.kpiId)
            
            results ++= List(row)
        }
        val sql = "delete from marketing_kpi where source = ? and game_code = ? and report_date = ? and kpi_id = ?"
        mysql.excuteBatchDelete(sql, results)
    }
}