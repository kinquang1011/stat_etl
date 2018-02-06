package  vng.ge.stats.report.report.adhoc
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.report.base.TReport
import vng.ge.stats.report.util.Constants
import org.apache.spark.sql.functions._
import vng.ge.stats.etl.utils.PathUtils
import vng.ge.stats.report.model.Schemas
import vng.ge.stats.report.model.Schemas.FieldName

/**
  * Created by canhtq on 2/9/17.
  */
class RevenueByUser(sparkSession: SparkSession, parameters: Map[String, String])
  extends TReport(sparkSession, parameters) {

  override def execute(mpDF: Map[String, DataFrame]): DataFrame = {
    import sparkSession.implicits._
    val paymentDF = mpDF(Constants.LogTypes.PAYMENT).select(FieldName.ID,FieldName.GROSS_AMT).groupBy(FieldName.ID).agg(sum(FieldName.GROSS_AMT).alias(FieldName.GROSS_AMT))
    //val paymentDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2017-01-03").select(FieldName.ID,FieldName.GROSS_AMT).groupBy(FieldName.ID).agg(sum(FieldName.GROSS_AMT).alias(FieldName.GROSS_AMT))
    val zeroRev:Double =0
    //val nDs = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/accregister_2/2017-01-03")

    //val newDF = nDs.selectExpr(FieldName.GAME_CODE,FieldName.ID, FieldName.LOG_DATE +" as " +FieldName.REG_DATE)
    val newDF = mpDF(Constants.LogTypes.ACC_REGISTER).selectExpr(FieldName.GAME_CODE,FieldName.ID, FieldName.LOG_DATE +" as " +FieldName.REG_DATE)
      .withColumn(FieldName.REV1,lit(zeroRev))
      .withColumn(FieldName.REV2,lit(zeroRev))
      .withColumn(FieldName.REV3,lit(zeroRev))
      .withColumn(FieldName.REV4,lit(zeroRev))
      .withColumn(FieldName.REV5,lit(zeroRev))
      .withColumn(FieldName.REV6,lit(zeroRev))
      .withColumn(FieldName.REV7,lit(zeroRev))
      .withColumn(FieldName.REV14,lit(zeroRev))
      .withColumn(FieldName.REV28,lit(zeroRev))
      .withColumn(FieldName.REV30,lit(zeroRev))
      .withColumn(FieldName.REV42,lit(zeroRev))
      .withColumn(FieldName.REV60,lit(zeroRev))
      .withColumn(FieldName.REV_TOTAL,lit(zeroRev))

    val detailRevDF = mpDF(Constants.LogTypes.REVENUE_BY_USER)
    //val detailRevDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/revenue_by_user/2017-01-02")
    val totalAccDF = detailRevDF.union(newDF)

    val joinDF = totalAccDF.as('total).join(paymentDF.as('pay), totalAccDF(FieldName.ID) === paymentDF(FieldName.ID), "left_outer")
      .selectExpr("total."+ FieldName.GAME_CODE,"total."+ FieldName.ID,"total."+ FieldName.REG_DATE
        ,"total."+ FieldName.REV1,"total."+ FieldName.REV2,"total."+ FieldName.REV3,"total."+ FieldName.REV4,"total."+ FieldName.REV5,"total."+ FieldName.REV6
        ,"total."+ FieldName.REV7,"total."+ FieldName.REV14,"total."+ FieldName.REV28
        ,"total."+ FieldName.REV30,"total."+ FieldName.REV42,"total."+ FieldName.REV60,"total."+ FieldName.REV_TOTAL
        ,"pay." +FieldName.GROSS_AMT)
      .withColumn(FieldName.LOG_DATE, lit(logDate + " 23:59:59")).withColumn("date_dif", datediff(col(FieldName.LOG_DATE),col(FieldName.REG_DATE)))

    val resultDF = joinDF.map(row=>{
      val gameCode= row.getAs[String](FieldName.GAME_CODE)
      val id=row.getAs[String](FieldName.ID)
      val regDate =row.getAs[String](FieldName.REG_DATE)
      val dif = row.getAs[Int]("date_dif")
      var rev1= row.getAs[Double](FieldName.REV1)
      var rev2= row.getAs[Double](FieldName.REV2)
      var rev3= row.getAs[Double](FieldName.REV3)
      var rev4= row.getAs[Double](FieldName.REV4)
      var rev5= row.getAs[Double](FieldName.REV5)
      var rev6= row.getAs[Double](FieldName.REV6)
      var rev7= row.getAs[Double](FieldName.REV7)
      var rev14= row.getAs[Double](FieldName.REV14)
      var rev28= row.getAs[Double](FieldName.REV28)
      var rev30= row.getAs[Double](FieldName.REV30)
      var rev42= row.getAs[Double](FieldName.REV42)
      var rev60= row.getAs[Double](FieldName.REV60)
      var revTotal= row.getAs[Double](FieldName.REV_TOTAL)
      var amt= row.getAs[Double](FieldName.GROSS_AMT)

      revTotal +=amt
      if(dif<=0){
        rev1 +=amt
        rev2 +=amt
        rev3 +=amt
        rev4 +=amt
        rev5 +=amt
        rev6 +=amt
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<2){
        rev2 +=amt
        rev3 +=amt
        rev4 +=amt
        rev5 +=amt
        rev6 +=amt
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<3){
        rev3 +=amt
        rev4 +=amt
        rev5 +=amt
        rev6 +=amt
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<4){
        rev4 +=amt
        rev5 +=amt
        rev6 +=amt
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<5){
        rev5 +=amt
        rev6 +=amt
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<6){
        rev6 +=amt
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<7){
        rev7 +=amt
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<14){
        rev14 +=amt
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<28){
        rev28 +=amt
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<30){
        rev30 +=amt
        rev42 +=amt
        rev60 +=amt
      }else if(dif<42){
        rev42 +=amt
        rev60 +=amt
      }else if(dif<60){
        rev60 +=amt
      }

      (gameCode,id,regDate
        ,rev1,rev2,rev3,rev4,rev5,rev6,rev7,rev14,rev28,rev30,rev42,rev60, revTotal)
    }).toDF(FieldName.GAME_CODE,FieldName.ID,FieldName.REG_DATE
      ,FieldName.REV1,FieldName.REV2,FieldName.REV3,FieldName.REV4,FieldName.REV5,FieldName.REV6,FieldName.REV7,FieldName.REV14,FieldName.REV28,FieldName.REV30,FieldName.REV42,FieldName.REV60, FieldName.REV_TOTAL)
    val dataSrc = PathUtils.getSourceFolderName(source)
    val path:String = PathUtils.getParquetPath(gameCode,logDate = logDate,logDir,sourceFolder = dataSrc,dataFolder =Constants.LogTypes.REVENUE_BY_USER)
    writer.mode("overwrite").writeParquet(resultDF,path)
    sparkSession.emptyDataFrame
  }

  override def write(df: DataFrame): Unit = {

    // make savable List

  }
}