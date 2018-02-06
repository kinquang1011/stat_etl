#!bin/bash

gameCode=ddd2mp2
calcId=id
sourceLog=ingame
path=data
group=game
from=2017-06-10
to=2017-06-11
report="1-2-3-4-5-6-7-8-9"
timing="a1,a3,a7,a14,a30,ac7,ac30"
logDir=/ge/warehouse
shellRunDate=`date +%Y-%m-%d`
d=$from
while [ "$d" \< "$to" ]; do
  echo $d

cmd="/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
        --class vng.ge.stats.report.job.Runner \
        --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 2g --executor-cores 1 --num-executors 4 \
        --conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar \
        --files hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/hive-site.xml \
        /home/fairy/oozie/canhtq/bundle/report/game_kpi/lib/stats-etlr-1.0.jar  \
        game_code=$gameCode log_date=$d calc_id=$calcId source=$sourceLog group_id=$group job_name=rerun-spark2-report report_number=$report run_timing=$timing log_dir=$logDir"

echo $cmd >> "/home/fairy/logs/rerun-report-$gameCode-$shellRunDate.log"

$cmd

d=$(date -I -d "$d + 1 day")

done
