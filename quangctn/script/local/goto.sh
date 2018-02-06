#!/bin/bash
type=$1
case "$1" in
"etl") cd /home/quangctn/source/etl_report/new/stats-etlr;;
"ui")  cd /home/quangctn/source/ui/stats-ui;;
"script_ui") cd /home/quangctn/source/ui/script;;
esac
