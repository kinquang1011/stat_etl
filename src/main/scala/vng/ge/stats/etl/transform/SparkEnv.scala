package vng.ge.stats.etl.transform

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext

import scala.util.Try

/**
  * Created by tuonglv on 15/02/2017.
  */
object SparkEnv {
    //private val conf: SparkConf = new SparkConf()
    //conf.setAppName("Formatter")
    //conf.set("spark.driver.allowMultipleContexts", "true")
    //private val spark = new SparkContext(conf)
    //private val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    //private val hiveContext = new HiveContext(spark)
    //hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //hiveContext.setConf("spark.sql.parquet.binaryAsString", "true")
    //val sparkSession1: SparkSession = new SparkSession()
    private val sparkSession: SparkSession = SparkSession.builder()
        .config("spark.driver.allowMultipleContexts", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.binaryAsString", "true")
        .enableHiveSupport()
        .getOrCreate()

    def getSparkContext: SparkContext = {
        sparkSession.sparkContext
    }

    def getSqlContext: SQLContext = {
        sparkSession.sqlContext
    }

    def getSparkSession: SparkSession = {
        sparkSession
    }

    def stop(): Unit = {
        //       Try {
        sparkSession.sparkContext.stop()
        sparkSession.stop()
        //       }
    }
}
