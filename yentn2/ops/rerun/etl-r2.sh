#!/bin/bash

function main(){
    sd="2017-02-10"
    ed="2017-04-24"
    interval=$3
    etlClass="NikkiSea"
    game_code="nikkisea"
    logType="ccu"
    pd=$sd
    break=0
    shellRunDate=`date +%Y-%m-%d`
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
        echo `date`", game_code: "$game_code", process_date:"$pd >> /home/fairy/logs/etl-$game_code$shellRunDate.log

/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 3g \
    --executor-memory 5g \
    --num-executors 2 \
    --executor-cores 2 \
    --conf spark.storage.memoryFraction=0 \
    --conf spark.shuffle.memoryFraction=1 \
    --queue production \
    --conf spark.yarn.jars=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar \
    --jars hdfs://c408.hadoop.gda.lo:8020/user/spark/share/lib/lib_20161117110230/datanucleus-api-jdo-3.2.6.jar,hdfs://c408.hadoop.gda.lo:8020/user/spark/share/lib/lib_20161117110230/datanucleus-core-3.2.10.jar,hdfs://c408.hadoop.gda.lo:8020/user/spark/share/lib/lib_20161117110230/datanucleus-rdbms-3.2.9.jar \
    --files hdfs://c408.hadoop.gda.lo:8020/user/spark/share/lib/lib_20161117110230/hive-site.xml,hdfs://c408.hadoop.gda.lo:8020/user/spark/share/lib/lib_20161117110230/spark-defaults.conf \
    --verbose \
    --class  vng.ge.stats.etl.transform.Factory \
    /home/fairy/oozie/canhtq/bundle/ops/rerun/lib/stats-etlr-1.0.jar className=$etlClass logDate=$pd logType=$logType

        if [ "$break" == "1" ];then
            break
        fi
    done
}
main