#!/bin/bash

sd=$1
ed=$2
className=$3
pd=$sd

while [ "$pd" != "$ed" ]
do
    echo $pd

   	 /home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit  \
		--class vng.ge.stats.etl.transform.Factory \
		--master yarn \
		--deploy-mode cluster \
		--queue production \
		--driver-memory 3000m \
		--executor-memory 5000m \
		--executor-cores 2 \
		--num-executors 2 \
		--conf spark.yarn.jar=hdfs://c408.hadoop.gda.lo:8020/user/fairy/libs/spark-2.1-hadoop-2.7.1/*.jar /home/fairy/ub/bundle/quangctn/etl/fishotthai/lib/stats-etlr-1.0.jar className=$className logDate=$pd logType=activity
    
    	pd=`date "+%Y-%m-%d" -d "$pd 1 day"`
done
