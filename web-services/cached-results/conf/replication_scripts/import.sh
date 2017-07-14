#!/bin/bash

HDFS_URI=${cached.results.hdfs.uri}
HDFS_DIR=$1
USER=${mysql.user.name}
PASS=${mysql.user.password}
DB=${mysql.dbname}
HOST=${mysql.host}

hadoop fs -fs ${HDFS_URI} -cat ${HDFS_DIR}/*.sql | mysql -u ${USER} --password=${PASS} -h ${HOST} -D ${DB}
