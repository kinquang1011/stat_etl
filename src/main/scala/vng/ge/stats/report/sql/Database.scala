package vng.ge.stats.report.sql

import com.google.common.base.Strings
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.ArrayList
import java.util.Arrays
import java.util.HashMap
import java.util.LinkedList
import java.util.List
import java.util.Map
import java.util.regex.Pattern
import scala.collection.JavaConversions._

abstract class Database(driver: String) {

    private val DBDRIVER_MSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    private val DBDRIVER_MYSQL = "com.mysql.jdbc.Driver"

    private val DBDRIVER_PGSQL = "org.postgresql.Driver"

    private val DBDRIVER_HSQLDB = "org.hsqldb.jdbc.JDBCDriver"

    private val STRING_PARAM_PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}")

    private var host: String = "10.60.22.2"
    private var port: String = "3306"
    private var dbName: String = "ubstats"
    private var dbUser: String = "ubstats"
    private var dbPwd: String = "pubstats"

    private var _driver: String = driver

    private val url = buildUrlConnection(_driver, host, port, dbName, true)

    protected var dbmanager: ManagerIF = ClientManager.getInstance(driver, url, dbUser, dbPwd)

    private def buildUrlConnection(databaseDriver: String,
        address: String,
        port: String,
        databaseName: String,
        sendStringParametersAsUnicode: Boolean): String = {
        if (databaseDriver == null || address == null || databaseName == null ||
            port == null) {
            return null
        }
        var url = ""
        if (databaseDriver == DBDRIVER_MSSQL) {
            url += "jdbc:sqlserver://" + address + ":" + port + ";database=" +
                databaseName
            if (!sendStringParametersAsUnicode) {
                url += ";sendStringParametersAsUnicode=false"
            }
            return url
        }
        if (databaseDriver == DBDRIVER_PGSQL) {
            url += "jdbc:postgresql://" + address + (if ("" == port) "" else ":" + port) +
                "/" +
                databaseName
            return url
        }
        if (databaseDriver == DBDRIVER_MYSQL) {
            url += "jdbc:mysql://" + address + (if ("" == port) "" else ":" + port) +
                "/" +
                databaseName +
                "?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8&"
            return url
        }
        url
    }

    protected def getConnection(): Connection = {
        val conn = this.dbmanager.borrowClient()

        conn
    }

    protected def releaseConnection(conn: Connection) {
        this.dbmanager.returnClient(conn)
    }

    protected def invalidConnection(conn: Connection) {
        this.dbmanager.invalidClient(conn)
    }

    protected def listParamByIndex(input: String): List[String] = {
        val result = new LinkedList[String]()
        val matcher = STRING_PARAM_PATTERN.matcher(input)
        while (matcher.find()) {
            result.add(matcher.group(1))
        }
        result
    }

    protected def prepareStatement(conn: Connection, sql: String, params: List[_]): PreparedStatement = {
        if (conn == null || Strings.isNullOrEmpty(sql)) {
            return null
        }
        val stmt = conn.prepareStatement(sql)
        if (params != null) {
            var index = 1
            for (param <- params) {
                if (param.isInstanceOf[String]) {
                    stmt.setString(index, param.asInstanceOf[String])
                } else if (param.isInstanceOf[java.lang.Integer]) {
                    stmt.setInt(index, param.asInstanceOf[java.lang.Integer])
                } else if (param.isInstanceOf[java.lang.Long]) {
                    stmt.setLong(index, param.asInstanceOf[java.lang.Long])
                } else if (param.isInstanceOf[java.lang.Float]) {
                    stmt.setFloat(index, param.asInstanceOf[java.lang.Float])
                } else if (param.isInstanceOf[java.lang.Double]) {
                    stmt.setDouble(index, param.asInstanceOf[java.lang.Double])
                } else {
                    stmt.setObject(index, param)
                }
                index += 1
            }
        }
        params.clear()
        stmt
    }

    /**
     * Date: 	2016-08-17
     * By: 		vinhdp
     */
    protected def prepareStatement(conn: Connection, sql: String): PreparedStatement = {

        if (conn == null || Strings.isNullOrEmpty(sql)) {
            return null
        }
        val stmt = conn.prepareStatement(sql)

        stmt
    }

    protected def prepareStatement(conn: Connection,
        sql: String,
        params: Map[String, _],
        useNullForMissingParams: Boolean): PreparedStatement = {
        if (conn == null || Strings.isNullOrEmpty(sql)) {
            return null
        }
        val paramByIndex = listParamByIndex(sql)
        val paramValueByIndex = new LinkedList[Any]()
        for (paramName <- paramByIndex) {
            if (params.containsKey(paramName)) {
                paramValueByIndex.add(params.get(paramName))
            } else {
                if (useNullForMissingParams) {
                    paramValueByIndex.add(null)
                } else {
                    throw new IllegalArgumentException("Missing value for param " + paramName)
                }
            }
        }
        var cleanSql = STRING_PARAM_PATTERN.matcher(sql).replaceAll("?")
        if (cleanSql == null || "" == cleanSql) {
            cleanSql = sql
        }
        paramByIndex.clear()
        prepareStatement(conn, cleanSql, paramValueByIndex)
    }

    protected def buildResultMapFromResultSet(rs: ResultSet,
        numColumns: Int,
        columnNames: scala.Array[String],
        columnTypes: scala.Array[Integer]): Map[String, Any] = {
        if (rs == null) {
            return null
        }
        if (!rs.next()) {
            return new HashMap()
        }
        val result = new HashMap[String, Any]()
        var i = 1
        while (i <= numColumns) {
            val columnName = columnNames(i - 1)
            val columnType = columnTypes(i - 1)
            var columnValue: Any = null

            if (columnType == 1 || columnType == 12) {
                columnValue = rs.getString(i)
            } else if (columnType == -6 || columnType == 5) {
                columnValue = rs.getShort(i)
            } else if (columnType == 4) {
                columnValue = rs.getInt(i)
            } else if (columnType == -5) {
                columnValue = rs.getLong(i)
            } else {
                columnValue = rs.getObject(i)
            }

            /* columnType match {
        case Types.CHAR | Types.VARCHAR => columnValue = rs.getString(i)
        case Types.TINYINT | Types.SMALLINT => columnValue = rs.getShort(i)
        case Types.INTEGER => columnValue = rs.getInt(i)
        case Types.BIGINT => columnValue = rs.getLong(i)
        case _ => columnValue = rs.getObject(i)
      }*/
            result.put(columnName, columnValue)
            i += 1
        }
        result
    }

    protected def buildResultListFromResultSet(rs: ResultSet): List[Map[String, Any]] = {
        if (rs == null) {
            return null
        }
        val result = new ArrayList[Map[String, Any]]()
        val rsMetaData = rs.getMetaData
        val columnNames = new ArrayList[String]()
        val columnTypes = new ArrayList[Integer]()
        val numColumns = rsMetaData.getColumnCount
        var i = 1
        while (i <= numColumns) {
            columnNames.add(rsMetaData.getColumnName(i))
            columnTypes.add(rsMetaData.getColumnType(i))
            i += 1
        }
        var entry = buildResultMapFromResultSet(rs, numColumns, columnNames.toArray(scala.Array.ofDim[String](0)),
            columnTypes.toArray(scala.Array.ofDim[Integer](0)))
        println("Is it working " + entry.isEmpty)
        while (entry != null && !entry.isEmpty) {
            result.add(entry)
            entry = buildResultMapFromResultSet(rs, numColumns, columnNames.toArray(scala.Array.ofDim[String](0)),
                columnTypes.toArray(scala.Array.ofDim[Integer](0)))
        }
        println(Arrays.toString(result.toArray()))
        rs.close()
        result
    }

    def checkIsDataExisted(sql: String, params: Map[String, _]): Boolean = {
        var rs = false
        var ps: PreparedStatement = null
        val conn = getConnection
        if (conn != null) {
            ps = prepareStatement(conn, sql, params, true)
            try {
                if (ps.execute()) {
                    val result = ps.getResultSet
                    if (result.next()) {
                        rs = true
                    }
                }
            } catch {
                case ex: SQLException => {
                    invalidConnection(conn)
                    throw ex
                }
            } finally {
                if (ps != null) {
                    ps.close()
                }
                releaseConnection(conn)
            }
        }
        rs
    }

    def executeStatement(sql: String, params: Map[String, _]): List[Map[String, Any]] = {
        var ps: PreparedStatement = null
        var dbResult: List[Map[String, Any]] = null
        val conn = getConnection
        if (conn != null) {
            ps = prepareStatement(conn, sql, params, true)
            try {
                if (ps.execute()) {
                    dbResult = buildResultListFromResultSet(ps.getResultSet)
                }
            } catch {
                case ex: SQLException => {
                    invalidConnection(conn)
                    throw ex
                }
            } finally {
                if (ps != null) {
                    ps.close()
                }
                releaseConnection(conn)
            }
        }
        dbResult
    }

    /**
     * Date:	2016-08-17
     * By:		vinhdp
     */
    def excuteBatchInsert(sql: String, data: scala.collection.immutable.List[scala.collection.mutable.Map[String, Any]]): Unit = {
        
        val conn = getConnection
        
        if (conn != null) {
        
            var ps = prepareStatement(conn, sql)
            var index: Int = 1

            for(element <- data){

                index = 1
                for(value <- element) {
                    
                    ps.setObject(index, value._2)
                    index = index + 1
                }
                
                ps.addBatch()
            }
            
            ps.executeBatch()
            ps.close()
        }
        
        releaseConnection(conn)
    }
    
    /**
     * Date:	2016-08-17
     * By:		vinhdp
     */
    def excuteBatchDelete(sql: String, data: scala.collection.immutable.List[scala.collection.mutable.Map[String, Any]]): Unit = {
        
        val conn = getConnection
        
        if (conn != null) {
        
            var ps = prepareStatement(conn, sql)
            var index: Int = 1
            
            for(element <- data){
                
                index = 1
                for(value <- element) {
                    
                    ps.setObject(index, value._2)
                    index = index + 1
                }
                
                ps.addBatch()
            }
            ps.executeBatch()
            ps.close()
        }
        
        releaseConnection(conn)
    }
    
    def executeUpdate(sql: String, params: Map[String, _]): java.lang.Boolean = {
        var ps: PreparedStatement = null
        val conn = getConnection
        if (conn != null) {
            ps = prepareStatement(conn, sql, params, true)
            try {
                if (ps.executeUpdate() > 0) {
                    return true
                }
            } catch {
                case ex: SQLException => {
                    invalidConnection(conn)
                    throw ex
                }
            } finally {
                if (ps != null) {
                    ps.close()
                }
                releaseConnection(conn)
            }
        }
        false
    }

    def executeUpdate(conn: Connection, sql: String, params: Map[String, _]): java.lang.Boolean = {
        var ps: PreparedStatement = null
        if (conn != null) {
            ps = prepareStatement(conn, sql, params, true)
            try {
                ps = prepareStatement(conn, sql, params, true)
                if (ps.executeUpdate() > 0) {
                    return true
                }
            } catch {
                case ex: SQLException => throw ex
            } finally {
                if (ps != null) {
                    ps.close()
                }
            }
        }
        false
    }

    def getDbConnection(): Connection = getConnection
}
