#!/bin/bash
path=/home/fairy/ub/bundle/quangctn/etl/etl_daily/lib/
mvn clean install
scp target/stats-etlr-1.0.jar fairy@10.60.43.15:$path
