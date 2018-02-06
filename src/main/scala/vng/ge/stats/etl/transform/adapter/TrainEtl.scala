package vng.ge.stats.etl.transform.adapter

import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.adapter.base.{Formatter, TmpFormatter}
import org.apache.spark.sql.functions._
import scala.collection.mutable

/**
  * Created by canhtq on 04/04/2017.
  */

case class TestPerson(name: String, age: Long, salary: Double)
class TrainEtl extends TmpFormatter("train")  {
  def start(args: Array[String]): Unit = {
    initParameters(args)
    var logDate: String = ""

    for (x <- args) {
      val xx = x.split("=")
      if (xx(0).contains("logDate")) {
        logDate = xx(1)
      }
    }
    this -> otherETL(logDate,"","") -> close
  }
  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {
    val ds = sparkSession.read.option("header","true").csv("/user/fairy/train/2017.03.revenue.csv")
    val sumDs = ds.groupBy("reg_date").agg(sum("rev").as("rev"))
    storeInParquet(sumDs,Constants.TMP_DIR + s"/train/$logDate")
  }
}
