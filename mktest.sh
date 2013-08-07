#!/bin/bash
./smart-bk.py --remove  --sid=1
./smart-bk.py --remove  --sid=2
./smart-bk.py --remove  --sid=3
#./smart-bk.py --add --time=16:05 --backup-type=rsync --source-host=england --dest-host=bahamas --source-dir=/etc/ --dest-dir=/data/backup 
#./smart-bk.py --add --time=20:30 --backup-type=dbdump --source-host=japan --dest-host=romania --source-dir=/mnt/koji --dest-dir=/data/backup 
./smart-bk.py --add --time=8:30 --backup-type=rsync --source-host=bahamas --dest-host=bahamas --source-dir=/etc/ --dest-dir=/data/backup/etc/ --source-user=backup --dest-user=backup
./smart-bk.py --expire --sid=1
./smart-bk.py --expire --sid=2
./smart-bk.py --expire --sid=3
