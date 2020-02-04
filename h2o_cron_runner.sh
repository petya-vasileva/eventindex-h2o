#!/bin/bash
source /etc/profile
. hadoop-setconf.sh lxhadoop

export CPATH=/afs/cern.ch/user/a/atlasdba/oei/h2o/*:/afs/cern.ch/user/a/atlasdba/oei/*:./jdbc/*:/opt/hadoop/conf/etc/lxhadoop/hadoop.lxhadoop:$(hadoop classpath)

cd oei

now=$(date "+%Y-%m-%d %H:%M:%S")
day=$(date "+%Y-%m-%d")

# 1. Check if the job is running
RUNNING=/user/atlasdba/h2o_todos/job_started
hdfs dfs -test -e $RUNNING
if [ $? = 0 ]
 then
    echo "Job is still running!"
    echo $now " Job is still running!" >> h2o_logs/log$day
    hdfs dfs -text /user/atlasdba/h2o_todos/job_started
    exit 1;
else
    echo "********************  H2O started " $now " ********************" >> h2o_logs/log$day
    echo "Job in progress since $now" | hdfs dfs -appendToFile - /user/atlasdba/h2o_todos/job_started
fi

# 2. Generate list of new datasets for the period of the last execution until the current moment
last_exec=$(hdfs dfs -cat /user/atlasdba/h2o_todos/job_last_exec)
last_exec_time=$(date --date="$last_exec" '+%Y-%m-%d %H:%M:%S') 
echo "last executed: $last_exec"
echo "Get validated datasets for the period from $last_exec to $now." >> h2o_logs/log$day
javac -cp $CPATH GetValidatedDatasets.java && java -Xmx1024m -cp $CPATH:$PWD GetValidatedDatasets "$last_exec_time" "$now" "h2o_todos/h2o_todo_list.txt" 

# 3. Note the current time and date that will be used as a starting point for the next import
echo $now | hdfs dfs  -put -f - /user/atlasdba/h2o_todos/job_last_exec

# 4. Compile                                                                                                                                 
javac -cp $CPATH H2O.java                                                                                                                                                       
# 5. Import                                                                              
fc=0
ic=0
sc=0
FILENAME=/user/atlasdba/h2o_todos/h2o_todo_list.txt
for f in `hdfs dfs -text $FILENAME`; do
    java -Xmx512m -cp $CPATH:$PWD H2O "$f" "$now" -Djava.security.egd=file:/dev/./urandom -Dsecurerandom.source=file:/dev/./urandom 2>&1 >> h2o_logs/log$day
    c1=$?
    echo "Begin import for $f" >> h2o_logs/log$day
    if [ $c1 -eq 0 ]; then
      ic=$((ic+1))
    elif [ $c1 -eq 201 ]; then
      sc=$((sc+1))
      #echo "Dataset is skipped, because it does not exist in COMA." >> h2o_logs/log$day
    else
      fc=$((fc+1))
      echo $f | hdfs dfs -appendToFile - /user/atlasdba/h2o_failed/failed_once
    fi

done;

echo "Imported:" $ic "datasets"
echo "Failed:" $fc "datasets"
echo "Skipped:" $sc "datasets"
echo "For details or in case of failures please check the log: /afs/cern.ch/user/a/atlasdba/oei/h2o_logs/log$day"
echo "***************  H2O finished import which was started on " $now " ***************" >> h2o_logs/log$day

# 6. Rename the imported file
hdfs dfs -mv $FILENAME $FILENAME"_imported_at_$(date '+%Y-%m-%d_%H.%M')"
echo "File renamed to " $FILENAME"_imported_at_$(date '+%Y-%m-%d_%H.%M')"

# 7. Notify that the job finished by deleting the job_started file
hdfs dfs -rm /user/atlasdba/h2o_todos/job_started
