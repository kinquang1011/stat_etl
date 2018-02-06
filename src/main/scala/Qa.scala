import org.apache.spark.sql.SparkSession

/**
  * Created by canhtq on 14/07/2017.
  */
object Qa {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.parquet.binaryAsString", "true")
    .enableHiveSupport()
    .getOrCreate()

  def checkActive(gameCode:String, logDate: String,locate:String ="fairy"): Unit ={
    var path:String = s"/ge/warehouse/$gameCode/$logDate"
    if(locate.equalsIgnoreCase("fairy")){
      println(path)
    }
  }
}
