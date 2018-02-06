package vng.ge.stats.report.sql

import java.sql.Connection
import java.sql.DriverManager
import java.util.HashMap
import java.util.Map
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import PureClientManager._
//remove if not needed
import scala.collection.JavaConversions._

object PureClientManager {

  private val createLock_ = new ReentrantLock()

  private var INSTANCES: Map[String, PureClientManager] = new HashMap()

  def getInstance(driver: String, 
      url: String, 
      user: String, 
      password: String): ManagerIF = {
    val key = driver + ":" + url + ":" + user + ":" + password
    if (!INSTANCES.containsKey(key)) {
      createLock_.lock()
      try {
        if (!INSTANCES.containsKey(key)) {
          INSTANCES.put(key, new PureClientManager(driver, url, user, password))
        }
      } finally {
        createLock_.unlock()
      }
    }
    INSTANCES.get(key).asInstanceOf[ManagerIF]
  }
}

class PureClientManager(private var driver: String, 
    private var url: String, 
    private var user: String, 
    private var password: String) extends ManagerIF {

  def borrowClient(): Connection = {
    try {
      Class.forName(this.driver)
      return DriverManager.getConnection(this.url + "user=" + this.user + "&password=" + this.password)
    } catch {
      case ex: Exception => 
    }
    null
  }

  def returnClient(client: Connection) {
    try {
      client.close()
    } catch {
      case ex: Exception => 
    }
  }

  def invalidClient(client: Connection) {
    try {
      client.close()
    } catch {
      case ex: Exception => 
    }
  }
}
