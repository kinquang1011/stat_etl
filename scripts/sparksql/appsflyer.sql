
val app = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/appsflyer/solu2/2016-07-01")

val n = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/accregister_2/2016-07-01")

val pay = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2016-07-07")

val pay = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2016-07-01")

val pay = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2016-07-0{1,2,3,4,5,6,7}")

val rawApp = spark.read.parquet("/ge/fairy/warehouse/appsflyer/install3/2016-07-01").where("app_id in ('com.vng.cuuam','id1089824208')").select("appsflyer_device_id", "idfa", "android_id", "platform", "media_source")

val extractId = udf {(os: String, aid: String, did: String) =>
var id = did
if(os.toLowerCase() == "android"){
id = aid
}
id
}

val appsflyer = rawApp.withColumn("id", extractId(col("platform"), col("android_id"), col("idfa")))

val payn = pay.as('p).join(n.as('n), pay("id") === n("id")).select("p.*")

val p = pay.selectExpr("id", "cast (net_amt as long) as net_amt").groupBy("id").agg(sum("net_amt").as("total"))

val joinDF = app.as('a).join(p.as('p), app("userID") === p("id"), "left_outer")

val resultDF = joinDF.groupBy("platform", "media_source").agg(count("platform").as("install"), sum("total").as("rev7")).show
val resultDF = joinDF.groupBy("log_date").agg(count("appFlyer_id").as("install"), sum("total").as("rev7"))


val extractOs = udf {(os: String) =>
var osLower = ""
if(os != null ) osLower = os.toLowerCase()
osLower match {
case "android"    => "android"
case "ios"        => "ios"
case "iphone os"  => "ios"
case "iphone"     => "ios"
case _            => ""
}
}

+--------+----------------------+
|platform|count(DISTINCT userId)|
+--------+----------------------+
|android |1106                  |
|ios     |6044                  |
+--------+----------------------+

val login = spark.read.json("/ge/gamelogs/sdk/2016-07-01/CACK_Login_InfoLog/CACK_Login_InfoLog-2016-07-01.gz")
val distinctLogin = login.orderBy(asc("userId"), asc("device_os")).dropDuplicates("userId").select("android_id", "device_id", "userId")

login.orderBy(asc("userId"), asc("device_os")).dropDuplicates("userId").select("android_id", "device_id", "userId").groupBy("userId").agg(count("android_id").as('a), count("device_id").as('d)).where("a > 0 and d > 0").count

val n = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/accregister_2/2016-07-01")
val appn = app.as('a).join(n.as('n), app("userID") === n("id")).select("a.*")
val payn = pay.as('a).join(n.as('n), pay("id") === n("id")).select("a.*")

=============================================================================
val app = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/appsflyer/solu3/2016-07-01")
val n = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/accregister_2/2016-07-01")

val pay = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2016-07-0{1,2,3,4,5,6,7}")

val payn = pay.as('p).join(n.as('n), pay("id") === n("id")).select("p.*")

val p = payn.selectExpr("id", "cast (net_amt as long) as net_amt").groupBy("id").agg(sum("net_amt").as("total"))

val joinDF = app.as('a).join(p.as('p), app("userID") === p("id"), "left_outer")

joinDF.groupBy("platform", "media_source").agg(count("platform").as("install"), sum("total").as("rev7")).show

=============================================================================
val totalInstallDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/appsflyer/solu3/*")

// drop duplicate
val totalDF = totalInstallDF.orderBy("userID", "install_time").dropDuplicates("userId")
val newDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/accregister_2/2016-07-0{1,2,3,4,5,6,7}")
val payDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2016-07-0{1,2,3,4,5,6,7}")

val newDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/accregister_2/2016-07-05")
val payDF = spark.read.parquet("/ge/warehouse/cack/ub/sdk_data/payment_2/2016-07-0{5,6,7}")

val r = d.collect().groupBy(row => (row.getString(0), row.getString(2)))
            .map{ row =>
                val logDate = row._1._1
                val mediaSource = row._1._2
                val values = row._2

                var mpOsPu = Map[String, Any]()
                var mpOsRev = Map[String, Any]()

                values.map { value =>
                    val os = value.getString(1)
                    val pu = value.getLong(3)
                    val rev = value.getDouble(4)

                    mpOsPu += (os -> pu)
                    mpOsRev += (os -> rev)
                }

                mpMediaPu += (mediaSource -> mpOsPu)
                mpMediaRev += (mediaSource -> mpOsRev)
            }
