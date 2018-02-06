package vng.ge.stats.report.sql

import java.sql.Connection
import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.commons.pool.impl.GenericObjectPoolFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ClientPoolByHost._
//remove if not needed
import scala.collection.JavaConversions._

object ClientPoolByHost {

  private val log = LoggerFactory.getLogger(classOf[ClientPoolByHost])

  val DEFAULT_MAX_ACTIVE = 20

  val DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED = -1L

  val DEFAULT_MAX_IDLE = 5
}

class ClientPoolByHost(driver: String, 
    url: String, 
    user: String, 
    password: String) {

  private var clientFactory: ClientObjectFactory = new ClientObjectFactory(driver, url, user, password)

  private var maxActive: Int = 20

  private var maxIdle: Int = 20

  private var maxWaitTimeWhenExhausted: Long = -1L

  private var pool: GenericObjectPool = createPool()

  this.pool.setMinEvictableIdleTimeMillis(50000L)

  this.pool.setTimeBetweenEvictionRunsMillis(55000L)

  def close() {
    try {
      this.pool.close()
    } catch {
      case e: Exception => log.error("Unable to close pool", e)
    }
  }

  private def createPool(): GenericObjectPool = {
    val poolFactory = new GenericObjectPoolFactory(this.clientFactory, this.maxActive, 1.toByte, this.maxWaitTimeWhenExhausted, 
      this.maxIdle)
    val p = poolFactory.createPool().asInstanceOf[GenericObjectPool]
    p.setTestOnBorrow(true)
    p.setTestWhileIdle(true)
    p.setMaxIdle(-1)
    p
  }

  def borrowClient(): Connection = {
    var client: Connection = null
    try {
      client = this.pool.borrowObject().asInstanceOf[Connection]
    } catch {
      case e: Exception => log.error("Uncaught exception", e)
    }
    client
  }

  def returnObject(client: Connection) {
    try {
      this.pool.returnObject(client)
    } catch {
      case e: Exception => log.error("Uncaught exception", e)
    }
  }

  def invalidClient(client: Connection) {
    try {
      this.pool.invalidateObject(client)
    } catch {
      case e: Exception => log.error("Uncaught exception", e)
    }
  }
}
