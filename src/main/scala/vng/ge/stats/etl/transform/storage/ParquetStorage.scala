package vng.ge.stats.etl.transform.storage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import vng.ge.stats.etl.constant.Constants
import vng.ge.stats.etl.transform.storage.schema.Schema
import vng.ge.stats.etl.utils.SchemaUtil

/**
 * Created by tuonglv on 27/12/2016.
 */
class ParquetStorage (_schema:Schema, _outputPath:String) extends Storage () {
    var schema: Schema = _schema
    var outputPath: String = _outputPath
    var writeMode: String = "overwrite"

    def setSchema(_schema: Schema): Unit = {
        schema = _schema
    }

    def setOutput(_outputPath: String): Unit = {
        outputPath = _outputPath
    }

    def setWriteMode(_writeMode: String): Unit = {
        writeMode = _writeMode
    }

    def store(ds: DataFrame, spark: SparkContext, sqlContext: SQLContext): Unit = {
        if (schema == null) {
            schema = new Schema {
                //override var schema: List[List[Any]] = SchemaUtil.getAllStringListSchema(ds)
                override var schema: List[List[Any]] = SchemaUtil.getDefaultDsSchema(ds)
            }
        }
        if (ds != null) {
            var fields: Array[String] = Array()
            ds.schema.foreach { col =>
                fields = fields ++ Array(col.name)
            }
            val fieldDataType = getConvertSql(fields)
            val storeDS = convertDataType(ds, fieldDataType)
            storeDS.coalesce(1).write.mode(writeMode).format("parquet").save(outputPath)
        } else {
            storeSchemaWithoutData(spark, sqlContext)
        }
    }

    def multiStore(ds: DataFrame): Unit = {

    }

    private def getConvertSql(fields: Array[String]): Array[String] = {
        var arrReturn: Array[String] = Array()
        schema.schema.foreach { list =>
            val field = list(0)
            val dataType = list(1)
            val defaultValue = list(2)
            if (fields.contains(field)) {
                val t = "cast (" + field + " as " + dataType + ") as " + field
                arrReturn = arrReturn ++ Array(t)
            } else {
                val t = "cast (" + defaultValue + " as " + dataType + ") as " + field
                arrReturn = arrReturn ++ Array(t)
            }
        }
        arrReturn
    }

    private def getDefaultSchema: StructType = {
        var aF: Array[StructField] = Array()
        schema.schema.foreach { list =>
            val fieldName: String = list.head.toString
            val fieldType = list(1)
            fieldType match {
                case Constants.DATA_TYPE.DOUBLE =>
                    aF = aF ++ Array(StructField(fieldName, DoubleType, nullable = true))
                case Constants.DATA_TYPE.LONG =>
                    aF = aF ++ Array(StructField(fieldName, LongType, nullable = true))
                case Constants.DATA_TYPE.INTEGER =>
                    aF = aF ++ Array(StructField(fieldName, IntegerType, nullable = true))
                case Constants.DATA_TYPE.STRING =>
                    aF = aF ++ Array(StructField(fieldName, StringType, nullable = true))
            }
        }
        val _schema = StructType(aF)
        _schema
    }

    def storeSchemaWithoutData(sc: SparkContext, sqlContext: SQLContext): Unit = {
        val b: RDD[Row] = sc.makeRDD(Seq(Row("qtbns", "sysxp", "dzhrp"), Row("zravq"), Row("zhawa")))
        val schemaWithoutData: DataFrame = sqlContext.createDataFrame(b, getDefaultSchema)
        val schemaWithoutData1 = schemaWithoutData.filter("game_code='rhdpaxtgtp'")
        val parquetField = getConvertSql(Array("k"))
        val write = convertDataType(schemaWithoutData1, parquetField)
        write.coalesce(1).write.mode(writeMode).format("parquet").save(outputPath)
    }

    def convertDataType(ds: DataFrame, p: Array[String]): DataFrame = {
        //ds.select(p.head, p.tail: _*)
        ds.selectExpr(p:_*)
    }
    //equivalent: https://spark.apache.org/docs/2.1.0/api/scala/index.html#org.apache.spark.sql.UDFRegistration
    def convertDataType1(ds: DataFrame, p: Array[String]): DataFrame = {
        val length = p.length
        val returnDF = length match {
            case 1 =>
                ds.selectExpr(p(0))
            case 2 =>
                ds.selectExpr(p(0), p(1))
            case 3 =>
                ds.selectExpr(p(0), p(1), p(2))
            case 4 =>
                ds.selectExpr(p(0), p(1), p(2), p(3))
            case 5 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4))
            case 6 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5))
            case 7 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6))
            case 8 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7))
            case 9 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))
            case 10 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))
            case 11 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10))
            case 12 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11))
            case 13 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12))
            case 14 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13))
            case 15 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14))
            case 16 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15))
            case 17 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16))
            case 18 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17))
            case 19 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18))
            case 20 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19))
            case 21 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20))
            case 22 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21))
            case 23 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22))
            case 24 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23))
            case 25 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23), p(24))
            case 26 =>
                ds.selectExpr(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23), p(24), p(25))
        }
        returnDF
    }
}