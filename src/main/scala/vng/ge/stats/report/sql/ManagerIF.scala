package vng.ge.stats.report.sql

import java.sql.Connection
//remove if not needed
import scala.collection.JavaConversions._

abstract trait ManagerIF {

  def borrowClient(): Connection

  def returnClient(paramConnection: Connection): Unit

  def invalidClient(paramConnection: Connection): Unit
}