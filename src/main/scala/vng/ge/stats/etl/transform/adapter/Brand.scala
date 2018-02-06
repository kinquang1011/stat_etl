package vng.ge.stats.etl.transform.adapter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import vng.ge.stats.etl.transform.adapter.base.FairyFormatter
import vng.ge.stats.etl.utils.Common

import scala.util.matching.Regex


/**
  * Created by quangctn on 08/02/2017.
  */
class Brand extends FairyFormatter("brand") {

  def start(args: Array[String]): Unit = {
    initParameters(args)
    var logDate: String = ""
    for (x <- args) {
      val xx = x.split("=")
      if (xx(0).contains("logDate")) {
        logDate = xx(1)
      }
    }
    this -> otherETL(logDate, "", "") -> close
  }

  override def otherETL(logDate: String, hourly: String, logType: String): Unit = {


  }

  private val getBrand2 = udf { (deviceInfo: String) => {
    var brand = ""
    if (deviceInfo != "") {
      val pattern = new Regex("((Build.BRAND.*?)_)")
      val result = (pattern findAllIn deviceInfo).mkString("")
      if (!result.equalsIgnoreCase("")) {
        //        Common.logger("vao day")
        val _brand = result.split(":")(1)
        //        Common.logger("brand:"+ _brand)
        brand = _brand.substring(0, _brand.length() - 1).toLowerCase().trim()
      }
    }
    brand
  }
  }

  def testDeviceInfo(spark: SparkSession): DataFrame = {
    var df = spark.read.json("/ge/gamelogs/sdk/2017-12-10/AUM_Login_InfoLog-2017-12-10.gz")
    df = df.withColumn("brand", getBrand2(col("device_info")))
    df
  }

  private val getBrand = udf { (deviceInfo: String, device_os: String, device: String) => {
    val device_name = device.toLowerCase
    val device_list: Map[String, String] = Map(
      "device" -> "brand_phone",
      "f1w" -> "oppo",
      "r827" -> "oppo",
      "cph1605" -> "oppo",
      "n5206" -> "oppo",
      "a51f" -> "oppo",
      "a33w" -> "oppo",
      "oppo r11" -> "oppo",
      "r821" -> "oppo",
      "r7007" -> "oppo",
      "r7005" -> "oppo",
      "n5111" -> "oppo",
      "galaxia s2" -> "oppo",
      "r8006" -> "oppo",
      "cph1701" -> "oppo",
      "oppo a59s" -> "oppo",
      "a51w" -> "oppo",
      "cph1613" -> "oppo",
      "cph1609" -> "oppo",
      "oppo a57" -> "oppo",
      "r1011" -> "oppo",
      "cph1715" -> "oppo",
      "cph1607" -> "oppo",
      "cph1611" -> "oppo",
      "r2001" -> "oppo",
      "r823t" -> "oppo",
      "cph1723" -> "oppo",
      "1201" -> "oppo",
      "cph1725" -> "oppo",
      "oppo a33m" -> "oppo",
      "x9006" -> "oppo",
      "3001" -> "oppo",
      "6607" -> "oppo",
      "x9009" -> "oppo",
      "r7plusf" -> "oppo",
      "a37fw" -> "oppo",
      "r831" -> "oppo",
      "f1f" -> "oppo",
      "r815" -> "oppo",
      "r831k" -> "oppo",
      "a33fw" -> "oppo",
      "r829" -> "oppo",
      "a1601" -> "oppo",
      "r7kf" -> "oppo",
      "oppo a37m" -> "oppo",
      "cph1717" -> "oppo",
      "r8106" -> "oppo",
      "r7t" -> "oppo",
      "r8001" -> "oppo",
      "a37f" -> "oppo",
      "n1" -> "oppo",
      "402so" -> "sony",
      "g3311" -> "sony",
      "d5833" -> "sony",
      "c6916" -> "sony",
      "f5122" -> "sony",
      "e6653" -> "sony",
      "e6553" -> "sony",
      "c6902" -> "sony",
      "e5563" -> "sony",
      "d2305" -> "sony",
      "l35h" -> "sony",
      "c5503" -> "sony",
      "d6603" -> "sony",
      "e5663" -> "sony",
      "g3226" -> "sony",
      "xm50h" -> "sony",
      "c6602" -> "sony",
      "g3416" -> "sony",
      "c6603" -> "sony",
      "d5103" -> "sony",
      "lt25i" -> "sony",
      "d6653" -> "sony",
      "d6616" -> "sony",
      "e6883" -> "sony",
      "c6833" -> "sony",
      "d5322" -> "sony",
      "d6502" -> "sony",
      "f8332" -> "sony",
      "c6802" -> "sony",
      "g3116" -> "sony",
      "c2105" -> "sony",
      "g3312" -> "sony",
      "d2403" -> "sony",
      "e6633" -> "sony",
      "e6683" -> "sony",
      "g8142" -> "sony",
      "g8232" -> "sony",
      "f3216" -> "sony",
      "e2312" -> "sony",
      "c6903" -> "sony",
      "f3116" -> "sony",
      "e2303" -> "sony",
      "d2502" -> "sony",
      "e5333" -> "sony",
      "d6503" -> "sony",
      "e5653" -> "sony",
      "d5503" -> "sony",
      "c1905" -> "sony",
      "f3115" -> "sony",
      "e2115" -> "sony",
      "mediapad t1 8.0" -> "huawei",
      "t1-701u" -> "huawei",
      "kob-l09" -> "huawei",
      "mya-l22" -> "huawei",
      "mediapad 7 youth 2" -> "huawei",
      "huawei y541-u02" -> "huawei",
      "trt-l21a" -> "huawei",
      "huawei kii-l21" -> "huawei",
      "cam-l32" -> "huawei",
      "huawei vns-l62" -> "huawei",
      "huawei lua-l22" -> "huawei",
      "sch-i919u" -> "huawei",
      "huawei cun-l21" -> "huawei",
      "eva-l19" -> "huawei",
      "cam-l21" -> "huawei",
      "sm-j700f" -> "huawei",
      "v8510" -> "huawei",
      "ple-701l" -> "huawei",
      "g750-t20" -> "huawei",
      "huawei c199s" -> "huawei",
      "vtr-l29" -> "huawei",
      "bg2-u01" -> "huawei",
      "t1-a21l" -> "huawei",
      "rne-l22" -> "huawei",
      "huawei tit-u02" -> "huawei",
      "cro-u00" -> "huawei",
      "huawei vns-l31" -> "huawei",
      "huawei nmo-l31" -> "huawei",
      "huawei g610-u20" -> "huawei",
      "t1 7.0" -> "huawei",
      "huawei gra-l09" -> "huawei",
      "bll-l22" -> "huawei",
      "huawei cun-u29" -> "huawei",
      "huawei rio-cl00" -> "huawei",
      "lon-al00" -> "huawei",
      "mha-l29" -> "huawei",
      "ale-l21" -> "huawei",
      "h60-l21" -> "huawei",
      "huawei g750-t00" -> "huawei",
      "huawei lua-u22" -> "huawei",
      "huawei p6-t00" -> "huawei",
      "huawei c199" -> "huawei",
      "bgo-l03" -> "huawei",
      "chc-u01" -> "huawei",
      "huawei y625-u32" -> "huawei",
      "huawei g750-t01" -> "huawei",
      "c8817d" -> "huawei",
      "y541-u02" -> "huawei",
      "mix" -> "xiaomi",
      "mi-4c" -> "xiaomi",
      "redmi note 4" -> "xiaomi",
      "mi max 2" -> "xiaomi",
      "mi 4lte" -> "xiaomi",
      "mi 5x" -> "xiaomi",
      "mi 4w" -> "xiaomi",
      "mi 2s" -> "xiaomi",
      "mi 6" -> "xiaomi",
      "mi 3w" -> "xiaomi",
      "hm note 1ltew" -> "xiaomi",
      "redmi 4x" -> "xiaomi",
      "redmi note 5a" -> "xiaomi",
      "mi 5" -> "xiaomi",
      "redmi 4a" -> "xiaomi",
      "redmi note 4x" -> "xiaomi",
      "redmi note 2" -> "xiaomi",
      "mi 3c" -> "xiaomi",
      "mi a1" -> "xiaomi",
      "mi note lte" -> "xiaomi",
      "2014817" -> "xiaomi",
      "redmi 3" -> "xiaomi",
      "mi note 2" -> "xiaomi",
      "mi 4c" -> "xiaomi",
      "redmi note 3" -> "xiaomi",
      "redmi 4" -> "xiaomi",
      "mi max" -> "xiaomi",
      "redmi 3s" -> "xiaomi",
      "mi pad 2" -> "xiaomi",
      "mi note pro" -> "xiaomi",
      "u pulse" -> "wiko",
      "bloom" -> "wiko",
      "ridge" -> "wiko",
      "robby" -> "wiko",
      "lenny3" -> "wiko",
      "rainbow lite" -> "wiko",
      "lenny2" -> "wiko",
      "sunny2 plus" -> "wiko",
      "highway star" -> "wiko",
      "jerry" -> "wiko",
      "u feel" -> "wiko",
      "sunset2" -> "wiko",
      "rainbow" -> "wiko",
      "getaway" -> "wiko",
      "k-kool" -> "wiko",
      "highway signs" -> "wiko",
      "pulp" -> "wiko",
      "ridge fab 4g" -> "wiko",
      "rainbow jam" -> "wiko",
      "pulp fab" -> "wiko",
      "sunny" -> "wiko",
      "chm-u01" -> "honor",
      "g621-tl00" -> "honor",
      "che-tl00h" -> "honor",
      "chm-tl00h" -> "honor",
      "h30-l02" -> "honor",
      "r6007" -> "honor",
      "che2-l11" -> "honor",
      "dli-tl20" -> "honor",
      "plk-tl01h" -> "honor",
      "pra-tl10" -> "honor",
      "honor h30-l02" -> "honor",
      "che2-tl00m" -> "honor",
      "plk-al10" -> "honor",
      "pra-al00x" -> "honor",
      "nem-tl00h" -> "honor",
      "che2-tl00" -> "honor",
      "duk-al20" -> "honor",
      "plk-cl00" -> "honor",
      "bln-al40" -> "honor",
      "xt1585" -> "motorola",
      "xt1095" -> "motorola",
      "motoe2(4g-lte)" -> "motorola",
      "moto g (4)" -> "motorola",
      "xt1572" -> "motorola",
      "xt1058" -> "motorola",
      "xt1706" -> "motorola",
      "xt1079" -> "motorola",
      "moto c plus" -> "motorola",
      "xt1032" -> "motorola",
      "xt1042" -> "motorola",
      "xt1528" -> "motorola",
      "gt-i8262d" -> "motorola",
      "xt1068" -> "motorola",
      "xt1663" -> "motorola",
      "xt1052" -> "motorola",
      "xt1650" -> "motorola",
      "xt1570" -> "motorola",
      "xt1060" -> "motorola",
      "xt1254" -> "motorola",
      "xt1092" -> "motorola",
      "xt1562" -> "motorola",
      "moto c" -> "motorola",
      "xt1033" -> "motorola",
      "moto e (4) plus" -> "motorola",
      "xt1030" -> "motorola",
      "xt536" -> "motorola",
      "droid bionic" -> "motorola"
    )
    var brand = ""
    if (device_os.toLowerCase.equalsIgnoreCase("ios")) {
      brand = "Apple".toLowerCase().trim()
    } else {
      if (deviceInfo != "") {
        val pattern = new Regex("((Build.BRAND.*?)_)")
        val result = (pattern findAllIn deviceInfo).mkString("")
        if (!result.equalsIgnoreCase("")) {
          //        Common.logger("vao day")
          val _brand = result.split(":")(1)
          //        Common.logger("brand:"+ _brand)
          brand = _brand.substring(0, _brand.length() - 1).toLowerCase().trim()
        }
      } else {
        //    Common.logger("brand: "+_brand)
        //    Common.logger("device: "+device)

        if ((brand.equalsIgnoreCase("") && !device_name.equalsIgnoreCase(""))) {
          if (device_name.startsWith("SM".toLowerCase())
            || device_name.startsWith("SAMSUNG".toLowerCase())
            || device_name.startsWith("GT".toLowerCase())
            || device_name.startsWith("SC".toLowerCase())
            || device_name.startsWith("SGH".toLowerCase())
            || device_name.startsWith("SHV".toLowerCase())) {
            brand = "samsung".toLowerCase()
          } else {
            if (device_name.contains("vivo")) {
              brand = "vivo".toLowerCase()
            }
            else {
              if (device_name.contains("asus".toLowerCase())) {
                brand = "asus".toLowerCase()
              }
              else {
                if (device_name.contains("mobiistar".toLowerCase())) {
                  brand = "mobiistar".toLowerCase()
                }
                else {
                  if (device_name.startsWith("LG".toLowerCase())) {
                    brand = "lge".toLowerCase()
                  }
                  else {
                    if (device_name.startsWith("Lenovo".toLowerCase())) {
                      brand = "Lenovo".toLowerCase()
                    }
                    else {
                      if (device_name.startsWith("IM".toLowerCase())) {
                        brand = "vega".toLowerCase()
                      }
                      else {
                        if (device_name.startsWith("Masstel".toLowerCase())) {
                          brand = "Masstel".toLowerCase()
                        } else {
                          if (device_name.startsWith("Coolpad".toLowerCase())) {
                            brand = "Coolpad".toLowerCase()
                          }
                          else {
                            if (device_name.startsWith("FPT".toLowerCase())) {
                              brand = "FPT".toLowerCase()
                            }
                          }
                        }

                      }
                    }
                  }
                }
              }

            }
          }

        }
        else {
          if (brand.equalsIgnoreCase("") && !device_name.equalsIgnoreCase("")) {
            if (device_list.contains(device_name)) {
              brand = device_list(device_name)
            }
          }
        }
      }
    }
    brand
  }
  }
  private val getDeviceInfo = udf { (deviceInfo: String) => {
    var re = deviceInfo
    if (null == deviceInfo) {
      re = ""
    }
    re
  }

  }
  var getField = udf { (field1: String, field2: String) => {
    //    Common.logger("field 1:"+field1)
    //    Common.logger("field 2:"+field2)
    var re = field1
    if ((field1 == null || field1.trim.equalsIgnoreCase("")) &&
      (field2 != null && !field2.equalsIgnoreCase(""))) {
      //      Common.logger("change")
      re = field2
    }
    re
  }
  }

  def getBrandName(spark: SparkSession, game_code: String, logDate: String): DataFrame = {
    var path = s"/ge/gamelogs/sdk/$logDate/{AUM,OMG}_Login_InfoLog/{AUM,OMG}_Login_InfoLog-$logDate.gz"
    path = path.replace("[game_code]", game_code.toUpperCase)
    //    Common.logger("path: " + path)
    var df = spark.read.json(path)
    var fields: Array[String] = Array()
    df.schema.foreach { col =>
      fields = fields ++ Array(col.name)
    }

    if (!fields.contains("device_info")) {
      //      Common.logger("vao day")
      df = df.withColumn("device_info", lit(""))
    }
    df = df.withColumn("device_info", getDeviceInfo(col("device_info")))
    df = df.select("device_info", "device_os", "userID", "device")
    //    df.show()
    df = df.withColumn("brand", getBrand(col("device_info"), col("device_os")))
    df
  }


  def changeSdk(spark: SparkSession, logDate: String): DataFrame = {
    var df = spark.read.parquet("/ge/fairy/warehouse/sdk/daily/login_info/2017-11-28")
      .select("game_id", "user_id", "os", "device_name", "more_info.device_info")
    df = df.withColumn("brand", getBrand(col("device_info"), col("os"), col("device_name")))
    /* df = df.withColumn("brand", mapDeviceName_Brand(col("brand"), col("device_name")))
     val config = spark.read.option("header", true).csv("/tmp/quangctn/device_list/oppo.csv")
     df = df.as('a).join(config.as('b), lower(df("device_name")) === lower(config("device")), "left_outer").select("a.*", "b.device", "b.brand_phone")
     df = df.withColumn("brand", getField(col("brand"), col("brand_phone")))*/
    df
  }

  var mapDeviceName_Brand = udf { (_brand: String, device: String) => {
    val device_name = device.toLowerCase
    var re = _brand
    //    Common.logger("brand: "+_brand)
    //    Common.logger("device: "+device)

    if ((_brand.equalsIgnoreCase("") && !device_name.equalsIgnoreCase(""))) {
      if (device_name.startsWith("SM".toLowerCase())
        || device_name.startsWith("SAMSUNG".toLowerCase())
        || device_name.startsWith("GT".toLowerCase())
        || device_name.startsWith("SC".toLowerCase())
        || device_name.startsWith("SGH".toLowerCase())
        || device_name.startsWith("SHV".toLowerCase())) {
        re = "samsung".toLowerCase()
      } else {
        if (device_name.contains("vivo")) {
          re = "vivo".toLowerCase()
        }
        else {
          if (device_name.contains("asus".toLowerCase())) {
            re = "asus".toLowerCase()
          }
          else {
            if (device_name.contains("mobiistar".toLowerCase())) {
              re = "mobiistar".toLowerCase()
            }
            else {
              if (device_name.startsWith("LG".toLowerCase())) {
                re = "lge".toLowerCase()
              }
              else {
                if (device_name.startsWith("Lenovo".toLowerCase())) {
                  re = "Lenovo".toLowerCase()
                }
                else {
                  if (device_name.startsWith("IM".toLowerCase())) {
                    re = "vega".toLowerCase()
                  }
                  else {
                    if (device_name.startsWith("Masstel".toLowerCase())) {
                      re = "Masstel".toLowerCase()
                    } else {
                      if (device_name.startsWith("Coolpad".toLowerCase())) {
                        re = "Coolpad".toLowerCase()
                      }
                      else {
                        if (device_name.startsWith("FPT".toLowerCase())) {
                          re = "FPT".toLowerCase()
                        }
                      }
                    }

                  }
                }
              }
            }
          }

        }
      }

    }
    //    Common.logger("End: "+re)
    re
  }
  }
}

