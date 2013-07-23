#!/bin/bash
user="oatley"
./smart-bk.py --remove  --sid=1 --backup-user=$user
./smart-bk.py --remove  --sid=2 --backup-user=$user
./smart-bk.py --remove  --sid=3 --backup-user=$user
./smart-bk.py --add --time=12:30 --backup-type=rsync --source-host=england --dest-host=bahamas --source-dir=/etc/ --dest-dir=/data/backup --backup-user=$user
./smart-bk.py --add --time=20:30 --backup-type=dbdump --source-host=japan --dest-host=romania --source-dir=/mnt/koji --dest-dir=/data/backup --backup-user=$user
./smart-bk.py --add --time=8:30 --backup-type=snap --source-host=iraq --dest-host=england --source-dir=/etc/ --dest-dir=/data/backup --backup-user=$user
./smart-bk.py --expire --sid=1 --backup-user=$user
./smart-bk.py --expire --sid=2 --backup-user=$user
./smart-bk.py --expire --sid=3 --backup-user=$user
