package vng.ge.stats.report.sql

import java.sql.Connection
import java.sql.SQLException
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext

class DbMySql extends Database("com.mysql.jdbc.Driver") {

  protected override def getConnection(): Connection = {
    var retry = 3
    var ret = false
    var conn: Connection = null
    while (ret == false) {
      try {
        conn = this.dbmanager.borrowClient()
        val mysqlcon = conn.asInstanceOf[com.mysql.jdbc.Connection]
        mysqlcon.ping()
        ret = true
      } catch {
        case ex: SQLException => {
          ex.printStackTrace(System.out)
          if (conn != null) {
            this.invalidConnection(conn)
          }
        }
      } finally {
        retry -= 1
        if (retry <= 0) {
          //break
        }
      }
    }
    conn
  }


}