#!/bin/bash
source /etc/profile
. hadoop-setconf.sh lxhadoop

export CPATH=/afs/cern.ch/user/a/atlasdba/oei/h2o/*:/afs/cern.ch/user/a/atlasdba/oei/*:./jdbc/*:/opt/hadoop/conf/etc/lxhadoop/hadoop.lxhadoop:$(hadoop classpath)

echo "******* H2O CLEANING SCRIPT IS STARTING ********"
echo "HDFS logs older than 2 months will be deleted..."

dnow=$(date "+%s" -d "2 month ago")

for f in `hdfs dfs -ls /user/atlasdba/h2o_todos/ |  grep -v '^Found' |  sed -r 's/^.+\///'`; do
    extract_date=$(echo ${f:30:10}) 
    imp_date=$(date --date="$extract_date" '+%s' )   
    # Check if the file was created more than 2 months ago
    if (( imp_date < dnow )) 
    then
	hdfs dfs -rm -skipTrash /user/atlasdba/h2o_todos/$f 
    fi
done;


echo "H2O logs older than 2 months will be deleted from AFS folder h2o_logs/"
find oei/h2o_logs/  -type f -mtime +60 -exec rm {} \;
