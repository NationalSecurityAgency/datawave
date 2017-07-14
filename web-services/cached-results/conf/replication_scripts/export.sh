#!/bin/bash

HDFS_URI=${cached.results.hdfs.uri}
HDFS_DIR=${cached.results.export.dir}
USERNAME=${mysql.user.name}
PASS=${mysql.user.password}
DB=${mysql.dbname}
HOST=${mysql.host}
LOG_DIR=${jboss.log.dir}
WORK_DIR=${LOG_DIR}/cached_results_exports

DB_INFO=${WORK_DIR}/cached_results_tables.out
TABLE_DIRS=${WORK_DIR}/cached_results_hdfs_dirs.out
LOCK_FILE=${WORK_DIR}/cached_results_export.lock

if [[ ! -d ${WORK_DIR} ]]; then
    mkdir -p ${WORK_DIR}
fi

if [ -e $LOCK_FILE ]; then
    exit 0
fi
touch $LOCK_FILE

hadoop fs -fs ${HDFS_URI} -mkdir ${HDFS_DIR} 2>/dev/null

while [[ 1 -eq 1 ]]; do

    mysql -u ${USERNAME} -D ${DB} -h ${HOST} --password=${PASS} -s -N -e "select  tableName, view, alias, user,queryId from cachedResultsQuery where status = 'LOADED' OR status = 'AVAILABLE';" > $DB_INFO
    hadoop fs -fs ${HDFS_URI} -lsr ${HDFS_DIR} | awk '{ print $8 }' > $TABLE_DIRS

    declare -a lines
    i=0
    for line in `cat $DB_INFO`; do
      lines[$i]=$line
      (( i += 1 ))
    done
    
    LENGTH=${#lines[*]}
    i=0
    while (( $i <  $LENGTH )) ; do
    
      TABLE=${lines[$i]}
      (( i += 1 ))
      VIEW=${lines[$i]}
      (( i += 1 ))
      ALIAS=${lines[$i]}
      (( i += 1 ))
      USER=${lines[$i]}
      (( i += 1 ))
      QUERYID=${lines[$i]}
      (( i += 1 ))

    
      #Does table exist in TABLE_DIRS?
      grep $TABLE $TABLE_DIRS
      RETVAL=$?
      
      #Directory does not exist for this table
      if [[ 1 -eq $RETVAL ]]; then
      
        echo "Dumping $TABLE"
        #Dump the table, view, and cachedResultsQuery row
        mysqldump -u ${USERNAME} --password=${PASS} -h ${HOST} --skip-opt --create-options --extended-insert --quick --compress --set-charset --no-autocommit --tables ${DB} $TABLE $VIEW > ${WORK_DIR}/$TABLE.sql
        if [[ $? -ne 0 ]]; then echo "Error dumping table $TABLE"; continue; fi
        mysqldump -u ${USERNAME} --password=${PASS} -h ${HOST} --skip-opt --no-create-info --extended-insert --quick --compress --set-charset --no-autocommit --tables --where="tableName = '$TABLE'" ${DB} cachedResultsQuery > ${WORK_DIR}/${TABLE}_CRQ.sql 
        if [[ $? -ne 0 ]]; then echo "Error dumping table $TABLE"; continue;  fi
        
        #Create the directory in HDFS and move the files in.
        hadoop fs -fs ${HDFS_URI} -mkdir ${HDFS_DIR}/$USER/$TABLE.tmp
        hadoop fs -fs ${HDFS_URI} -touchz ${HDFS_DIR}/$USER/$TABLE.tmp/$ALIAS.alias
        hadoop fs -fs ${HDFS_URI} -touchz ${HDFS_DIR}/$USER/$TABLE.tmp/$VIEW.view
        hadoop fs -fs ${HDFS_URI} -touchz ${HDFS_DIR}/$USER/$TABLE.tmp/$QUERYID.queryId
        hadoop fs -fs ${HDFS_URI} -copyFromLocal ${WORK_DIR}/$TABLE.sql ${HDFS_DIR}/$USER/$TABLE.tmp/$TABLE.sql
        hadoop fs -fs ${HDFS_URI} -copyFromLocal ${WORK_DIR}/${TABLE}_CRQ.sql ${HDFS_DIR}/$USER/$TABLE.tmp/${TABLE}_CRQ.sql
        hadoop fs -fs ${HDFS_URI} -mv ${HDFS_DIR}/$USER/$TABLE.tmp ${HDFS_DIR}/$USER/$TABLE

        #Cleanup the temp files
        rm ${WORK_DIR}/$TABLE.sql
        rm ${WORK_DIR}/${TABLE}_CRQ.sql
              
      fi
    
    done

    echo "Sleeping..."
    sleep 30

done

rm $LOCK_FILE

