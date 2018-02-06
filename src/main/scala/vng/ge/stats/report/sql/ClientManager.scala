package vng.ge.stats.report.sql

import java.sql.Connection
import java.util.HashMap
import java.util.Map
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import org.apache.log4j.Logger
import ClientManager._
//remove if not needed
import scala.collection.JavaConversions._

object ClientManager {

  private val createLock_ = new ReentrantLock()

  private val instances = new HashMap[String, ClientManager]()

  private val logger_ = Logger.getLogger(classOf[ClientManager])

  def getInstance(driver: String, 
      url: String, 
      user: String, 
      password: String): ManagerIF = {
    val key = driver + ":" + url + ":" + user + ":" + password
    if (!instances.containsKey(key)) {
      createLock_.lock()
      try {
        if (!instances.containsKey(key)) {
          instances.put(key, new ClientManager(driver, url, user, password))
        }
      } finally {
        createLock_.unlock()
      }
    }
    instances.get(key).asInstanceOf[ManagerIF]
  }
}

class ClientManager(driver: String, 
    url: String, 
    user: String, 
    password: String) extends ManagerIF {

  private val commentClientPoolByHost = new ClientPoolByHost(driver, url, user, password)

  override def borrowClient(): Connection = {
    val client = this.commentClientPoolByHost.borrowClient()
    client
  }

  override def returnClient(client: Connection) {
    this.commentClientPoolByHost.returnObject(client)
  }

  override def invalidClient(client: Connection) {
    this.commentClientPoolByHost.invalidClient(client)
  }
}
