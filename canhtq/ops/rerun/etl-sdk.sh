#!/bin/bash
homedir=`pwd`

function main(){
    sd="2016-06-28"
    ed="2016-06-29"
    interval=$3
    game_code=360live
    pd=$sd
    break=0
    while [ true ]
    do
        sd=$pd
        pd=`date "+%Y-%m-%d" -d "$pd $interval day"`

        cp1=`date "+%s" -d"$pd"`
        cp2=`date "+%s" -d"$ed"`

        if [ $cp1 -gt $cp2 ];then
            pd=$ed
            break
        fi
        echo `date`", game_code: "$game_code", process_date:"$pd
        /usr/bin/spark-submit --class vng.stats.ub.normalizer.v2.SdkFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 3000m --executor-memory 3000m --executor-cores 1 --num-executors 6 \
        --conf spark.yarn.queue=production --conf spark.executor.instances=3 --conf spark.executor.memory=2g --conf spark.executor.cores=1 --conf spark.driver.memory=3g --conf spark.shuffle.memoryFraction=0.5 --conf spark.buffer.pageSize=2m --conf spark.driver.extraJavaOptions=-Dhdp.version= --conf spark.yarn.jar=hdfs:///user/spark/share/lib/lib_20161017014545/spark-assembly-1.6.2-hadoop2.7.1.jar --jars hdfs:///user/spark/share/lib/lib_20161017014545/datanucleus-api-jdo-3.2.6.jar,hdfs:///user/spark/share/lib/lib_20161017014545/datanucleus-core-3.2.10.jar,hdfs:///user/spark/share/lib/lib_20161017014545/datanucleus-rdbms-3.2.9.jar --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml \
         /home/fairy/oozie/canhtq/libs/stats-spark.jar gameCode=$game_code outputFolder=sdk_data inputPath=/ge/gamelogs/sdk logDate=$pd

        if [ "$break" == "1" ];then
            break
        fi
    done
}

main
