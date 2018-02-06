package vng.ge.stats.report.sql

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import org.apache.commons.pool.PoolableObjectFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ClientObjectFactory._
//remove if not needed
import scala.collection.JavaConversions._

object ClientObjectFactory {

  private val log = LoggerFactory.getLogger(classOf[ClientObjectFactory])
}

class ClientObjectFactory(private var driver: String, 
    private var url: String, 
    private var user: String, 
    private var password: String) extends PoolableObjectFactory {

  override def activateObject(arg0: AnyRef) {
  }

  override def destroyObject(obj: AnyRef) {
    if (obj == null) {
      return
    }
    val client = obj.asInstanceOf[Connection]
    client.close()
  }

  override def makeObject(): AnyRef = {
    Class.forName(this.driver)
    val client = DriverManager.getConnection(this.url + "user=" + this.user + "&password=" + this.password)
    client
  }

  override def passivateObject(arg0: AnyRef) {
  }

  override def validateObject(obj: AnyRef): Boolean = {
    var result = true
    try {
      val client = obj.asInstanceOf[Connection]
      if (obj == null) {
        return result
      }
      result = (result) && (!client.isClosed)
    } catch {
      case ex: SQLException => result = false
    }
    result
  }
}
