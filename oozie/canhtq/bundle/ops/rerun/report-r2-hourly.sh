#!bin/bash

gameCode=gnm
date=2017-04-24
hourInDay="03"
rootDir=/ge/warehouse

/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
        --class vng.ge.stats.etl.adhoc.IngameReportHourly \
        --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 2g --executor-cores 1 --num-executors 4 \
        --conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark2/*.jar \
        --files hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/hive-site.xml \
        /home/fairy/oozie/canhtq/bundle/ops/rerun/lib/stats-etlr-1.0.jar  \
        gameCode=$gameCode logDate=$date  rootDir=$rootDir job_name=rerun-spark2-report hourInDay=$hourInDay




