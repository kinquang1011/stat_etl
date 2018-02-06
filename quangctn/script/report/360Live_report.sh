#!bin/bash

gameCode=360live
calcId=id
source=ingame
from=2017-03-27
to=2017-03-29

d=$from
while [ "$d" \< "$to" ]; do
  echo $d

/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit --class vng.ge.stats.report.job.Runner --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 4g --num-executors 4 --conf spark.yarn.jars=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar /home/fairy/quangctn/spark_submit/report/lib/stats-etlr-1.0.jar game_code=$gameCode log_date=$d calc_id=$calcId source=$source group_id=game job_name=360live-report report_number=2-3-4-5-6-7-8-9 log_dir=/ge/fairy/warehouse

d=$(date -I -d "$d + 1 day")

done


