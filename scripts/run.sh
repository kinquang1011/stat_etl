#!bin/bash

gameCode=nikki
calcId=id
source=ingame
path=data

from=2017-01-01
to=2017-03-08

d=$from
while [ "$d" \< "$to" ]; do
  echo $d

~/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit --class vng.ge.stats.etl.transform.Factory --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 2g --executor-cores 1 --num-executors 4 --conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar --files hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/hive-site.xml /home/zdeploy/vinhdp/stats-etlr-1.0.jar className=CoccmSea logDate=$d

d=$(date -I -d "$d + 1 day")

done