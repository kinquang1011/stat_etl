#!bin/bash

homedir=`pwd`
private_key=/home/vinhdp/.ssh/id_rsa

table=$1

ssh -i $private_key root@10.60.22.2 << ENDSSH
sh /home/tuonglv/sync_db/dump_xxx.sh $table
ENDSSH

rm -f $homedir/$table.sql
scp -i $private_key root@10.60.22.2:/home/tuonglv/sync_db/$table.sql $homedir
scp -i $private_key $homedir/$table.sql root@10.40.1.21:/home/tuonglv/sync_db/$table.sql 

ssh -i $private_key root@10.40.1.21 << ENDSSH
sh /home/tuonglv/sync_db/restore_xxx.sh $table
ENDSSH


echo "DONE"
