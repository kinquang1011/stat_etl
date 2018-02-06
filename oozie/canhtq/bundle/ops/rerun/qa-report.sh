#!bin/bash

class=vng.ge.stats.report.report.qa.MtoSdk
from=2017-05-23
to=2017-05-24
logDir=/ge/gamelogs/sdk_sea
shellRunDate=`date +%Y-%m-%d`
d=$from
while [ "$d" \< "$to" ]; do
  echo $d

cmd="/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
        --class $class \
        --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 2g --executor-cores 1 --num-executors 4 \
        --conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar \
        --files hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/hive-site.xml \
        /home/fairy/oozie/canhtq/bundle/ops/rerun/lib/stats-etlr-1.0.jar  \
        logDir=$logDir log_date=$d"

echo $cmd >> "/home/fairy/logs/rerun-report-$gameCode-$shellRunDate.log"

$cmd

d=$(date -I -d "$d + 1 day")

done
