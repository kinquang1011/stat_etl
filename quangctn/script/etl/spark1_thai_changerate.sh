#!/bin/bash

sd=$1
ed=$2
pd=$sd
gameCode=$3
while [ "$pd" != "$ed" ]
do
    echo $pd

    ~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit \
        --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml \
        --class vng.stats.ub.normalizer.v2.SdkThaiFormatter \
        --master yarn \
        --deploy-mode cluster \
        --queue production \
        --driver-memory 3000m \
        --executor-memory 3000m \
        --executor-cores 2 \
        --num-executors 2 \
        --jars /home/fairy/libs/datanucleus-core-3.2.10.jar,/home/fairy/libs/datanucleus-api-jdo-3.2.6.jar,/home/fairy/libs/datanucleus-rdbms-3.2.9.jar /home/fairy/quangctn/libspark1/stats-spark.jar logDate=$pd gameCode=$gameCode outputFolder=sdk_data inputPath=/ge/gamelogs/sdk_thai changeRate=638

    pd=`date "+%Y-%m-%d" -d "$pd 1 day"`
done

