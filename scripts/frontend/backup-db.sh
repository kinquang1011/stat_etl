#!/bin/bash

homedir=`pwd`
private_key=/home/vinhdp/.ssh/id_rsa


ssh -i $private_key root@10.60.22.2 << ENDSSH
sh /home/tuonglv/sync_db/dump.sh
ENDSSH

rm -f $homedir/ubstats.sql
scp -i $private_key root@10.60.22.2:/home/tuonglv/sync_db/ubstats.sql $homedir
scp -i $private_key $homedir/ubstats.sql root@10.40.1.21:/home/tuonglv/sync_db/ubstats.sql

ssh -i $private_key root@10.40.1.21 << ENDSSH
sh /home/tuonglv/sync_db/restore.sh
ENDSSH


echo "DONE"