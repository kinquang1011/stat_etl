package vng.ge.stats.report.sql.mysql

import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.sql.DbMySql
import vng.ge.stats.report.util.{Constants, Logger}

import scala.collection.mutable.LinkedHashMap
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._


object MysqlGroupReport {
    
    var mysql: DbMySql = new DbMySql()
    var table = ""
    
    def getTableName(groupId: String, calcId: String): String = {
        
        if(calcId == "id"){
            
            groupId match {
                case Constants.GroupId.CHANNEL => {
                    
                    "channel_kpi"
                }
                case Constants.GroupId.PACKAGE => {
                    
                    "package_kpi"
                }
                case Constants.GroupId.SERVER => {
                    
                    "server_kpi_json"
                }
                case Constants.GroupId.COUNTRY => {
                    
                    "country_kpi_json"
                }
                case Constants.GroupId.OS => {
                    
                    "os_kpi"
                }
            }
        } else if(calcId == "did") {
            
            groupId match {
                case Constants.GroupId.CHANNEL => {
                    
                    "device_channel_kpi"
                }
                case Constants.GroupId.PACKAGE => {
                    
                    "device_package_kpi"
                }
                case Constants.GroupId.SERVER => {
                    
                    "device_server_kpi"
                }
                case Constants.GroupId.COUNTRY => {
                    
                    "device_country_kpi"
                }
            }
        } else {
            
            "unknow_table"
        }
    }

    def insert(lstOutput: List[KpiGroupFormat], groupId: String, calcId: String): Unit = {

        delete(lstOutput, groupId, calcId)
        
        var results = List[LinkedHashMap[String, Any]]()

        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()

            row += ("logDate" -> values.logDate)
            row += ("gameCode" -> values.gameCode)
            row += ("source" -> values.source)
            row += ("group" -> values.groupId)
            row += ("kpiId" -> values.kpiId)
            row += ("value" -> values.value)
            row += ("createDate" -> values.createDate)
            
            results ++= List(row)
        }
        
        table = getTableName(groupId, calcId)
    
        Logger.info("insert into " + table)
        var sql = "insert into " + table + " values (null, ?, ?, ?, ?, ?, ?, ?)"
        mysql.excuteBatchInsert(sql, results)
        //Common.logger("no insert into " + table)
    }
    
    def delete(lstOutput: List[KpiGroupFormat], groupId: String, calcId: String): Unit = {

        var results = List[LinkedHashMap[String, Any]]()
        
        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("kpiId" -> values.kpiId)
            
            results ++= List(row)
        }

        table = getTableName(groupId, calcId)
        Logger.info("delete from " + table)
        //Common.logger("no delete from " + table)
        
        var sql = "delete from " + table + " where source = ? and game_code = ? and report_date = ? and kpi_id = ?"
        mysql.excuteBatchDelete(sql, results)
    }
    
    def insertJson(lstOutput: List[KpiGroupFormat], groupId: String, calcId: String): Unit = {

        deleteJson(lstOutput, groupId, calcId)
        
        var results = List[LinkedHashMap[String, Any]]()
        var jsonObj = Map[String, Any]()
        var mp = LinkedHashMap[String, Any]()
        var json = ""
        
        var logDate = ""
        var gameCode = ""
        var source = ""
        var kpiId = 0
        var createDate = ""

        implicit val formats = net.liftweb.json.DefaultFormats
        
        lstOutput.groupBy(row => row.kpiId).map{
                value => 
                    
                    kpiId = value._1
                    jsonObj = Map[String, Any]()
                    
                    value._2.map{ 
                            x => 
                                logDate = x.logDate
                                gameCode = x.gameCode
                                source = x.source
                                createDate = x.createDate
                                jsonObj += (x.groupId -> x.value)
                    }
                    json = compact(render(decompose(jsonObj)))
                    Logger.info(json)
                    mp = LinkedHashMap("logDate" -> logDate, "gameCode" -> gameCode, "source" -> source, "kpiId" -> kpiId, "value" -> json, "createDate" -> createDate)
                    results ++= List(mp)
        }
        
        table = getTableName(groupId, calcId)
    
        Logger.info("insert into " + table)
        val sql = "insert into " + table + " (report_date, game_code, source, kpi_id, kpi_value, calc_date) values (?, ?, ?, ?, ?, ?)"
        mysql.excuteBatchInsert(sql, results)
    }
    
    def deleteJson(lstOutput: List[KpiGroupFormat], groupId: String, calcId: String): Unit = {

        var results = List[LinkedHashMap[String, Any]]()
        
        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("kpiId" -> values.kpiId)
            
            results ++= List(row)
        }

        table = getTableName(groupId, calcId)
        Logger.info("delete from " + table)
        
        val sql = "delete from " + table + " where source = ? and game_code = ? and report_date = ? and kpi_id = ?"
        mysql.excuteBatchDelete(sql, results)
    }
}