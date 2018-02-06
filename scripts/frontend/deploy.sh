#!/bin/bash

dir=/home/vinhdp/vngprojects/ub/ub-frontendvsmt
ssh_key=/home/vinhdp/.ssh/id_rsa

svn_c="true"
if [ "$1" = "nc" ];then
    svn_c="false"
fi

if [ "$svn_c" = "true" ];then
    update=`svn up $dir`
    if [ "$?" != "0" ];then
        echo "update fail" && exit 2
    fi
    echo $update
    commit=`svn commit -m " " $dir`
    if [ "$?" != "0" ];then
        echo "commit fail" && exit 3
    fi
    echo $commit
fi

dbc=$dir/trunk/application/frontend/config/database.php
checkdb=`cat $dbc | grep "\$mode = 'prod'" | grep "//"`
if [ "$checkdb" != "" ];then
    echo "database config is in local" && exit 4
fi
cc=$dir/trunk/application/frontend/config/config.php
checkc=`cat $cc | grep "kpi.stats.vng" | grep "^//"`
if [ "$checkc" != "" ];then
    echo "config is in local" && exit 5
fi

read -p "Are you sure (y/n)?" choice
case "$choice" in 
  y|Y ) echo "";;
  n|N ) echo "abort" && exit;;
  * ) echo "invalid" && exit;;
esac

sudo rsync -avr -e "ssh -i $ssh_key" $dir/trunk/ root@10.60.22.161:/var/www/kpiweb/

ssh -i $ssh_key root@10.60.22.161 << ENDSSH
sh /home/tuonglv/script/sync_to_171.sh
ENDSSH
echo "done"in/bash

