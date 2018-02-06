val extractChannel = udf {(channel: String) =>
var channelLower = ""
if(channel != null ) channelLower = channel.toLowerCase()

channelLower match {
case "play_store_playstore"    => Array("Android", "IAP")
case "app_store_appstore"        => Array("iOS", "IAP")
case _            => Array(channelLower, "MOL")
}
}

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import vng.ge.stats.etl.utils.DateTimeUtils
import scala.util.Try

val incrementTimeZone = udf {(date: String) =>
var newDateTime = ""
Try {
val timeStamp = DateTimeUtils.getTimestamp(date)
newDateTime = DateTimeUtils.getDate(timeStamp + 25200 * 1000)
}
newDateTime
}

val dfCountry = spark.read.parquet("/ge/warehouse/nikkisea/ub/sdk_data/payment_2/2017-{02-*,03-*}").withColumn("log_date", incrementTimeZone(col("log_date"))).withColumn("pay_channel", extractChannel(col("pay_channel"))).selectExpr("substring(log_date,0,10) as log_date", "id", "split(channel, '_')[1] as country", "pay_channel[0] as channel", "pay_channel[1] as gate_way", "gross_amt").groupBy("log_date","country", "gate_way", "channel").agg(sum("gross_amt").as('amt)).cache

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val totalByCountry = Window.partitionBy("log_date", "country")

val percentByCountry = dfCountry.withColumn("total", sum("amt") over totalByCountry).orderBy("log_date", "country").selectExpr("log_date", "country", "gate_way", "channel", "amt", "trans", "total", "round(amt/total * 100, 2) as percent").cache

val dfAll = spark.read.parquet("/ge/warehouse/nikkisea/ub/sdk_data/payment_2/2017-{02-*,03-*}").withColumn("log_date", incrementTimeZone(col("log_date"))).withColumn("pay_channel", extractChannel(col("pay_channel"))).selectExpr("substring(log_date,0,10) as log_date", "id", "split(channel, '_')[1] as country", "pay_channel[0] as channel", "pay_channel[1] as gate_way", "gross_amt").groupBy("log_date", "gate_way", "channel").agg(sum("gross_amt").as('amt)).cache
val totalByDate = Window.partitionBy("log_date")

val percentByAll = dfAll.withColumn("country", lit("All")).withColumn("total", sum("amt") over totalByDate).orderBy("log_date").selectExpr("log_date", "country", "gate_way", "channel", "amt", "trans", "total", "round(amt/total * 100, 2) as percent").cache

val results = percentByAll.union(percentByCountry).orderBy("log_date", "country", "gate_way", "channel")
results.coalesce(1).write.format("csv").option("header", "true").save("/user/fairy/vinhdp/nikkisea_channel_revenue")


/// new version
val dfCountry = spark.read.parquet("/ge/warehouse/nikkisea/ub/sdk_data/payment_2/2017-{02-*,03-*}").withColumn("pay_channel", extractChannel(col("pay_channel"))).selectExpr("substring(log_date,0,10) as log_date", "id", "split(channel, '_')[1] as country", "pay_channel[0] as channel", "pay_channel[1] as gate_way", "gross_amt", "trans_id").groupBy("log_date","country", "gate_way", "channel").agg(sum("gross_amt").as('amt), countDistinct("trans_id").as('trans)).cache
val dfAll = spark.read.parquet("/ge/warehouse/nikkisea/ub/sdk_data/payment_2/2017-{02-*,03-*}").withColumn("pay_channel", extractChannel(col("pay_channel"))).selectExpr("substring(log_date,0,10) as log_date", "id", "split(channel, '_')[1] as country", "pay_channel[0] as channel", "pay_channel[1] as gate_way", "gross_amt", "trans_id").groupBy("log_date", "gate_way", "channel").agg(sum("gross_amt").as('amt), countDistinct("trans_id").as('trans)).cache