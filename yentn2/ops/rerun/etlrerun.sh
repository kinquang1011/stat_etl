#!bin/bash

gameCode=ztm
etlClass=Ztm
from=2017-06-28
to=2017-06-30
logType=ccu
shellRunDate=`date +%Y-%m-%d`
d=$from

while [ "$d" \< "$to" ]; do
  echo $d

cmd="/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
        --class vng.ge.stats.etl.transform.Factory \
        --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 2g --executor-cores 1 --num-executors 4 \
        --conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar \
        --files hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/hive-site.xml \
        /home/fairy/oozie/canhtq/bundle/ops/rerun/lib/stats-etlr-1.0.jar  \
        className=$etlClass logDate=$d logType=$logType"

echo $cmd >> "/home/fairy/logs/rerun-etl-$etlClass-$shellRunDate.log"

$cmd

d=$(date -I -d "$d + 1 day")

done
