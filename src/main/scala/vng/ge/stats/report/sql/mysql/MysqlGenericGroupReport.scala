package vng.ge.stats.report.sql.mysql

import net.liftweb.json.Extraction._
import net.liftweb.json.JsonAST._
import net.liftweb.json.Printer._
import vng.ge.stats.report.model.KpiGroupFormat
import vng.ge.stats.report.sql.DbMySql
import vng.ge.stats.report.util.{Constants, Logger}

import scala.collection.mutable.LinkedHashMap


object MysqlGenericGroupReport {
    
    var mysql: DbMySql = new DbMySql()
    val table = "group_kpi_json"
    
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
                    Logger.info(json, tab = 3)
                    mp = LinkedHashMap("logDate" -> logDate, "gameCode" -> gameCode, "source" -> source, "groupId" -> groupId, "kpiId" -> kpiId, "value" -> json, "createDate" -> createDate)
                    results ++= List(mp)
        }
    
        Logger.info("insert into " + table, tab = 3)
        val sql = "insert into " + table + " (report_date, game_code, source, group_id, kpi_id, kpi_value, calc_date) values (?, ?, ?, ?, ?, ?, ?)"
        mysql.excuteBatchInsert(sql, results)
    }
    
    def deleteJson(lstOutput: List[KpiGroupFormat], groupId: String, calcId: String): Unit = {

        var results = List[LinkedHashMap[String, Any]]()
        
        for (values <- lstOutput) {

            var row = LinkedHashMap[String, Any]()
            
            row += ("source" -> values.source)
	        row += ("groupId" -> groupId)
            row += ("gameCode" -> values.gameCode)
            row += ("logDate" -> values.logDate)
            row += ("kpiId" -> values.kpiId)
            
            results ++= List(row)
        }
        Logger.info("delete from " + table, tab = 3)
        
        val sql = "delete from " + table + " where source = ? and group_id = ? and game_code = ? and report_date = ? and kpi_id = ?"
        mysql.excuteBatchDelete(sql, results)
    }
}