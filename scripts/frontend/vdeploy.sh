#!/bin/bash

rsync -avr -e 'ssh -i /home/vinhdp/.ssh/id_rsa' /home/vinhdp/vngprojects/ub/ub-frontendvsmt/trunk/ root@10.40.1.21:/data/data22/ub-frontendvsmt_c/trunk

ssh -i /home/vinhdp/.ssh/id_rsa root@10.40.1.21 << ENDSSH
sh /data/data22/script/smt_c_sync_to_22.sh
ENDSSH

echo "done"
