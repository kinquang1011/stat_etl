#!/bin/bash

etlClass=abc
logDate=2017-01-01

/home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1/bin/spark-submit \
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
--class  vng.ge.stats.etl.transform.Factory \
/home/fairy/oozie/canhtq/bundle/report/game_kpi/lib/stats-etlr-1.0.jar className=$etlClass logDate=$logDate
