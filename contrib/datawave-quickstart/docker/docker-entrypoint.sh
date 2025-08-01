#!/bin/env bash

/usr/bin/nohup /usr/sbin/sshd -D > /dev/null 2>&1 &

# export ENV variables set in Dockerfile so exec'd process has them
export USER
export HADOOP_IDENT_STRING
export HDFS_NAMENODE_USER
export HDFS_DATANODE_USER
export HDFS_SECONDARYNAMENODE_USER
export YARN_RESOURCEMANAGER_USER
export YARN_NODEMANAGER_USER
export DW_DATAWAVE_BUILD_PROFILE
export DW_DATAWAVE_INGEST_TEST_DATA_SKIP
export DW_MAVEN_REPOSITORY
export DW_WGET_OPTS
export JAVA_HOME
export PATH
export DW_BIND_HOST
export DW_ACCUMULO_BIND_HOST

export DW_ACCUMULO_URL
export DW_HADOOP_URL
export DW_WILDFLY_URL
export DW_ZOOKEEPER_URL

export DW_ACCUMULO_VERSION
export DW_HADOOP_VERSION
export DW_WILDFLY_VERSION
export DW_ZOOKEEPER_VERSION

export DW_ACCUMULO_DIST_SHA512_CHECKSUM
export DW_HADOOP_DIST_SHA512_CHECKSUM
export DW_WILDFLY_DIST_SHA512_CHECKSUM
export DW_ZOOKEEPER_DIST_SHA512_CHECKSUM

# use exec so signal handling works properly when container is stopped/restarted/etc
exec "$@"
