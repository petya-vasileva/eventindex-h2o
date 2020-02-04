#!/bin/bash
source /etc/profile
. hadoop-setconf.sh lxhadoop

export CPATH=/afs/cern.ch/user/a/atlasdba/oei/h2o/*:/afs/cern.ch/user/a/atlasdba/oei/*:./jdbc/*:/opt/hadoop/conf/etc/lxhadoop/hadoop.lxhadoop:$(hadoop classpath)

cd oei/
now=$(date "+%Y-%m-%d %H:%M:%S")
day=$(date "+%Y-%m-%d")

# 1. Check if the job is running
RUNNING=/user/atlasdba/h2o_todos/job_started
hdfs dfs -test -e $RUNNING
if [ $? = 0 ]
 then
    echo "Job is still running!"
    hdfs dfs -text /user/atlasdba/h2o_todos/job_started
    exit 1;
else
    echo "Job in progress since $now" | hdfs dfs -appendToFile - /user/atlasdba/h2o_todos/job_started
fi

# 2. Compile
javac -cp $CPATH H2O.java                       

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< " Import started $now " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" >> h2o_logs/failed_log$day
echo " "
echo " "

# 3. Check for datasets failed import 4 times
f4=/user/atlasdba/h2o_failed/failed_4times
hdfs dfs -test -e $f4
if [ $? = 0 ]
 then 
    echo "THERE ARE PROBLEMATIC DATASETS WHICH IMPORT FAIED 4 TIMES. PLEASE INVESTIGATE!"  >> h2o_logs/failed_log$day
    echo "PLEASE CHECK THE LOGS AND TAKE ACTIONS!!!"
    echo "When you finish, please execute the command: hdfs dfs -mv h2o_failed/failed_4times h2o_failed/failed_4times_imported_at_$(date '+%Y-%m-%d_%H.%M')"
fi

# 4. Check for datasets failed import 3 times
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORT DATASETS THAT FAILED 3 TIMES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" >> h2o_logs/failed_log$day
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORT DATASETS THAT FAILED 3 TIMES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

f3=/user/atlasdba/h2o_failed/failed_thrice
hdfs dfs -test -e $f3
if [ $? = 0 ]
 then
   fc=0
   ic=0
   sc=0
   for f in `hdfs dfs -text $f3`; do
       java -Xmx512m -cp $CPATH:$PWD H2O "$f" "$now"  -Djava.security.egd=file:/dev/./urandom -Dsecurerandom.source=file:/dev/./urandom 2>&1 >> h2o_logs/failed_log$day 
       c1=$?
       if [ $c1 -eq 0 ]; then
	   ic=$((ic+1))
	   echo "Dataset " $f "imported successfully" >> h2o_logs/failed_log$day
       elif [ $c1 -eq 201 ]; then
           sc=$((sc+1))
           #echo"Dataset is skipped, because it does not exist in COMA." >> h2o_logs/failed_log$day
       else
	   fc=$((fc+1))
	   echo $f | hdfs dfs -appendToFile - /user/atlasdba/h2o_failed/failed_4times
	   echo "The following dataset failed import 4 times: " $f
	   echo "The following dataset failed import 4 times: " $f >> h2o_logs/failed_log$day
	   echo "PLEASE CHECK THE LOGS AND TAKE ACTIONS!"
	   echo "When you finish, please execute the command: hdfs dfs -mv h2o_failed/failed_4times h2o_failed/failed_4times_imported_at_$(date '+%Y-%m-%d_%H.%M')"
       fi
   done;
   echo "Imported:" $ic "datasets"
   echo "Failed:" $fc "datasets"
   echo "Skipped:" $sc "datasets"
   hdfs dfs -mv $f3 $f3"_imported_at_$(date '+%Y-%m-%d_%H.%M')"
   echo "File renamed to " $f3"_imported_at_$(date '+%Y-%m-%d_%H.%M')" >> h2o_logs/failed_log$day
else
    echo "There are no datasets for import">> h2o_logs/failed_log$day
fi

# 5. Check for datasets failed import 2 times
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORT DATASETS THAT FAILED 2 TIMES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" >> h2o_logs/failed_log$day
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORT DATASETS THAT FAILED 2 TIMES >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

f2=/user/atlasdba/h2o_failed/failed_twice
hdfs dfs -test -e $f2
if [ $? = 0 ]
 then
   fc=0
   ic=0
   sc=0
   for f in `hdfs dfs -text $f2`; do
       java -Xmx512m -cp $CPATH:$PWD H2O "$f" "$now"  -Djava.security.egd=file:/dev/./urandom -Dsecurerandom.source=file:/dev/./urandom 2>&1 >> h2o_logs/failed_log$day 
       c1=$?
       if [ $1 -eq 0 ]; then
           ic=$((ic+1))
	   echo "Dataset " $f "imported successfully" >> h2o_logs/failed_log$day
       elif [ $c1 -eq 201 ]; then
           sc=$((sc+1))
           #echo"Dataset is skipped, because it does not exist in COMA." >> h2o_logs/failed_log$day
       else
           fc=$((fc+1))
           echo $f | hdfs dfs -appendToFile - /user/atlasdba/h2o_failed/failed_thrice
       fi
   done;

   echo "Imported:" $ic "datasets"
   echo "Failed:" $fc "datasets"
   echo "Skipped:" $sc "datasets"
   hdfs dfs -mv $f2 $f2"_imported_at_$(date '+%Y-%m-%d_%H.%M')"
   echo "File renamed to " $f2"_imported_at_$(date '+%Y-%m-%d_%H.%M')" >> h2o_logs/failed_log$day
else
    echo "There are no datasets for import" >> h2o_logs/failed_log$day
fi

# 6. Check for datasets failed import 1 time
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORT DATASETS THAT FAILED 1 TIME >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" >> h2o_logs/failed_log$day
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<< IMPORT DATASETS THAT FAILED 1 TIME >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

f1=/user/atlasdba/h2o_failed/failed_once
hdfs dfs -test -e $f1
if [ $? = 0 ]
 then
   fc=0
   ic=0
   sc=0
   for f in `hdfs dfs -text $f1`; do
       java -Xmx512m -cp $CPATH:$PWD H2O "$f" "$now"  -Djava.security.egd=file:/dev/./urandom -Dsecurerandom.source=file:/dev/./urandom  2>&1 >> h2o_logs/failed_log$day 
       c1=$?
       if [ $c1 -eq 0 ]; then
           ic=$((ic+1))
	   echo "Dataset " $f "imported successfully" >> h2o_logs/failed_log$day
       elif [ $c1 -eq 201 ]; then
           sc=$((sc+1))
	   echo "Dataset is skipped, because it does not exist in COMA." >> h2o_logs/failed_log$day
       else
           fc=$((fc+1))
           echo $f | hdfs dfs -appendToFile - /user/atlasdba/h2o_failed/failed_twice
       fi
   done;

   echo "Imported:" $ic "datasets"
   echo "Failed:" $fc "datasets"
   echo "Skipped:" $sc "datasets"
   hdfs dfs -mv $f1 $f1"_imported_at_$(date '+%Y-%m-%d_%H.%M')"
   echo "File renamed to " $f1"_imported_at_$(date '+%Y-%m-%d_%H.%M')" >> h2o_logs/failed_log$day
else
    echo "There are no datasets for import" >> h2o_logs/failed_log$day
fi

echo "In case of failures please check the logs in /afs/cern.ch/user/a/atlasdba/oei/h2o_logs/failed_log$day"
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< " Import finished $now " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" >> h2o_logs/failed_log$day
echo " "
echo " "

# 7. Notify that the job finished by deleting the job_started file
hdfs dfs -rm /user/atlasdba/h2o_todos/job_started
