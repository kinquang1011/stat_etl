package vng.ge.stats.etl.transform

import org.apache.spark.sql.DataFrame
import vng.ge.stats.etl.transform.storage.ParquetStorage
import vng.ge.stats.etl.transform.storage.schema.Schema
import vng.ge.stats.etl.utils.{PathUtils, SchemaUtil}

/**
 * Created by tuonglv on 27/12/2016.
 */
object Factory {
    def main(args: Array[String]) {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            val xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }
        val className = mapParameters("className")
        val obj = Class.forName("vng.ge.stats.etl.transform.adapter." + className).newInstance.asInstanceOf[ {def start(args: Array[String]): Unit}]
        obj.start(args)
    }

    /**
      * create parquetFile from dataFrame. If schema is null, all fields will be StringType
      *
      * @param sourceFolder .Ex: data, sdk_data, data_hourly...
      * @param dataFolder   .Ex: payment, payment_2, activity...
      */
    def createParquetDs(ds: DataFrame, gameCode: String, logDate: String, rootDir: String, sourceFolder: String, dataFolder: String, schema: Schema = null): Unit = {
        val outputPath = PathUtils.getParquetPath(gameCode, logDate, rootDir, sourceFolder, dataFolder)
        val parquetStorage = new ParquetStorage(schema, outputPath)
        parquetStorage.store(ds, SparkEnv.getSparkContext, SparkEnv.getSqlContext)
    }
}