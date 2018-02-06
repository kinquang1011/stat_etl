#!/bin/bash

group=$1
type=$2

oozie job -config /home/fairy/ub/bundle/vinhdp/$group/$type/job-bundle.properties -run -doas zdeploy > ./newest_bundle_id.log
bid=`cat ./newest_bundle_id.log`
echo "bundle_id = "$bid
