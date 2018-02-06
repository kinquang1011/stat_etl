#!/bin/bash
homedir=`pwd`

function main(){
    sd=$1
    ed=$2
    interval=$3
    game_code=$4
    etlClass=$5

    pd=$sd
    break=0
    logType=ccu
    jarPath=/home/fairy/canhtq/libs/stats-spark.jar
    while [ true ]
    do
        sd=$pd
        pd=`date "+%Y-%m-%d" -d "$pd $interval day"`

        cp1=`date "+%s" -d"$pd"`
        cp2=`date "+%s" -d"$ed"`

        if [ $cp1 -gt $cp2 ];then
            pd=$ed
            break=1
        fi

        echo `date`", game_code: "$game_code", sd:"$sd", ed:"$pd

        /home/fairy/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class $etlClass --master yarn --deploy-mode cluster --queue production --driver-memory 3000m --executor-memory 3000m --executor-cores 1 --num-executors 6 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml,hdfs:///user/spark/share/lib/lib_20161017014545/spark-defaults.conf  --jars /home/fairy/libs/lib_20161117110230/datanucleus-core-3.2.10.jar,/home/fairy/libs/lib_20161117110230/datanucleus-api-jdo-3.2.6.jar,/home/fairy/libs/lib_20161117110230/datanucleus-rdbms-3.2.9.jar $jarPath gameCode=$game_code logType=$logType logDate=$pd
        if [ "$break" == "1" ];then
            break
        fi
    done
}

main 2017-03-21 2017-03-22 1 hpt vng.stats.ub.normalizer.hcatalog.HptFormatter
