#!/bin/sh
#

START_TIME=$SECONDS
logDate=$1

~/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
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
    --class  vng.ge.stats.report.job.Runner \
    /home/fairy/vinhdp/stats-etlr-1.0.jar \
    game_code=3qmobile log_date=2017-03-03 calc_id=id source=ingame group_id=game job_name=test-spark2-report report_number=1-2-3-4-5-6-7-8-9 run_timing=a1 log_dir=/ge/warehouse


ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo $ELAPSED_TIME

~
~
~
~
~
~
~
~