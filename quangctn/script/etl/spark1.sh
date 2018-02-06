#!/bin/bash

sd=$1
ed=$2
pd=$sd
className=$3
while [ "$pd" != "$ed" ]
do
    echo $pd

    ~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit \
        --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml \
        --class $className \
        --master yarn \
        --deploy-mode cluster \
        --queue production \
        --driver-memory 3000m \
        --executor-memory 3000m \
        --executor-cores 2 \
        --num-executors 2 \
        --jars /home/fairy/libs/datanucleus-core-3.2.10.jar,/home/fairy/libs/datanucleus-api-jdo-3.2.6.jar,/home/fairy/libs/datanucleus-rdbms-3.2.9.jar hdfs://c408.hadoop.gda.lo:8020/user/fairy/ub/bundle/tuonglv/etl/etl_daily/lib/stats-spark.jar logDate=$pd

    pd=`date "+%Y-%m-%d" -d "$pd 1 day"`
done

