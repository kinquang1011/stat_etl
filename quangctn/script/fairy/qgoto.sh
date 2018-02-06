#!/bin/bash
type=$1
case "$1" in
"bundle") cd /home/fairy/ub/bundle/quangctn;;
"etl")  cd /home/fairy/quangctn/spark_submit/etl;;
"report") cd /home/fairy/quangctn/spark_submit/report;;
"shell") cd /home/fairy/ub/tools/spark-2.1.0-bin-hadoop-2.7.1;;
esac
