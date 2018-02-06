#!/bin/bash

#nohup sh etl-sdk2.sh &

from=2017-05-18
to=2017-05-19

etlClass="sdk.SiamplayIndo"
game_code="siamplayindo"
sdk_game_code="siamplayindo"
sdk_source="sdk_sea"
timezone="0"
changeRate="1724"
shellRunDate=`date +%Y-%m-%d`
d=$from
while [ "$d" \< "$to" ]; do
  echo $d

cmd="/home/fairy/ub/tools/spark-2.0.1-scala.2.11/bin/spark-submit \
        --class vng.ge.stats.etl.transform.Factory \
        --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 8g --executor-cores 1 --num-executors 1 \
        --conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar \
        --files hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/hive-site.xml \
        /home/fairy/oozie/canhtq/bundle/ops/rerun/lib/stats-etlr-1.0.jar  \
        className=$etlClass gameCode=$game_code logDate=$d sdkGameCode=$sdk_game_code changeRate=$changeRate timezone=$timezone sdkSource=$sdk_source"

echo $cmd >> "/home/fairy/logs/rerun-etl-$game_code-$shellRunDate.log"

$cmd

d=$(date -I -d "$d + 1 day")

done
