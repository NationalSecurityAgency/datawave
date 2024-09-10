#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	READLINK_CMD="python -c 'import os,sys;print os.path.realpath(sys.argv[1])'"
	MKTEMP_OPTS="-t $0"
else
	READLINK_CMD="readlink -f"
	MKTEMP_OPTS=""
fi
THIS_SCRIPT=`eval $READLINK_CMD $0`
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh
. ../ingest/job-cache-env.sh

#read from the datawave metadata table to create the edge key version file and save it locally with the rest of the config files
./create-edgekey-version-cache.sh --update ../../config

# Swap the job cache directory
echo Old job cache dir is $JOB_CACHE_DIR
OLD_JOB_CACHE_DIR=$JOB_CACHE_DIR
OLD_SUFFIX=""
NEW_SUFFIX=""
if [[ ${JOB_CACHE_DIR: -1} == "A" ]]; then
    JOB_CACHE_DIR=${JOB_CACHE_DIR:0:${#JOB_CACHE_DIR}-1}B
    OLD_SUFFIX="A"
    NEW_SUFFIX="B"
else
   JOB_CACHE_DIR=${JOB_CACHE_DIR:0:${#JOB_CACHE_DIR}-1}A
   OLD_SUFFIX="B"
   NEW_SUFFIX="A"
fi
echo New job cache dir is $JOB_CACHE_DIR

BEFORE=$(basename $OLD_JOB_CACHE_DIR)
AFTER=$(basename $JOB_CACHE_DIR)

sed s%${BEFORE}%${AFTER}% job-cache-env.sh > job-cache-env.tmp

. ../ingest/ingest-libs.sh

date

# prepare a directory with links to all of the files/directories to put into the jobcache
tmpdir=`mktemp -d $MKTEMP_OPTS`
trap 'rm -r -f "$tmpdir"; exit $?' INT TERM EXIT
for f in ${CLASSPATH//:/ }; do
    if [ -e $f ]; then
        fname=${f/*\//}
        # determine the actual path
        f=$(eval $READLINK_CMD $f)
        ln -s $f $tmpdir/$fname
    else
        echo "*** WARNING: The classpath points to a location that does not exist: $f ***"
    fi
done

# determine the number of processors we can use
if [[ `uname` == "Darwin" ]]; then
	declare -i CPUS=`sysctl machdep.cpu.thread_count | awk '{print $2}'`
else
	declare -i CPUS=`cat /proc/cpuinfo | grep processor | awk '{print $3}' | sort -n | tail -1`
fi
# lets use twice the number of processors
CPUS=`echo "$LOAD_JOBCACHE_CPU_MULTIPLIER * $CPUS" | bc`

# Remove the ingest new job cache directory, if it already exists, and then load files into it...

if $INGEST_HADOOP_HOME/bin/hadoop fs -conf $INGEST_HADOOP_CONF/hdfs-site.xml -fs $INGEST_HDFS_NAME_NODE -test -d $INGEST_HDFS_NAME_NODE$JOB_CACHE_DIR > /dev/null 2>&1 ; then
   echo "Replacing ingest job cache directory: $INGEST_HDFS_NAME_NODE$JOB_CACHE_DIR"
   $INGEST_HADOOP_HOME/bin/hadoop fs -conf $INGEST_HADOOP_CONF/hdfs-site.xml -fs $INGEST_HDFS_NAME_NODE -rm -r $INGEST_HDFS_NAME_NODE$JOB_CACHE_DIR
else
   echo "Creating ingest job cache directory: $INGEST_HDFS_NAME_NODE$JOB_CACHE_DIR"
fi
# copyFromLocal needs the parent directory chain to exist, so ensure that is in place before doing a multi-threaded copyFromLocal
$INGEST_HADOOP_HOME/bin/hadoop fs -conf $INGEST_HADOOP_CONF/hdfs-site.xml -fs $INGEST_HDFS_NAME_NODE -mkdir -p $INGEST_HDFS_NAME_NODE${JOB_CACHE_DIR}/..
$INGEST_HADOOP_HOME/bin/hadoop fs -conf $INGEST_HADOOP_CONF/hdfs-site.xml -fs $INGEST_HDFS_NAME_NODE -copyFromLocal -t $CPUS ${tmpdir} $INGEST_HDFS_NAME_NODE${JOB_CACHE_DIR}
# Only do setrep for an hdfs filesystem. Others, such as local or abfs, don't support or need the replication to be set.
[[ "$INGEST_HDFS_NAME_NODE" == "hdfs://"* ]] && $INGEST_HADOOP_HOME/bin/hadoop fs -conf $INGEST_HADOOP_CONF/hdfs-site.xml -setrep -R ${JOB_CACHE_REPLICATION} $INGEST_HDFS_NAME_NODE${JOB_CACHE_DIR}

########### We need this section to allow running the map file merger on the warehouse cluster ##########
if [[ "$WAREHOUSE_HDFS_NAME_NODE" != "$INGEST_HDFS_NAME_NODE" ]]; then
   if $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -conf $WAREHOUSE_HADOOP_CONF/hdfs-site.xml -fs $WAREHOUSE_HDFS_NAME_NODE -test -d $WAREHOUSE_HDFS_NAME_NODE$JOB_CACHE_DIR > /dev/null 2>&1 ; then
      echo "Replacing warehouse job cache directory: $WAREHOUSE_HDFS_NAME_NODE$JOB_CACHE_DIR"
      $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -conf $WAREHOUSE_HADOOP_CONF/hdfs-site.xml -fs $WAREHOUSE_HDFS_NAME_NODE -rm -r $WAREHOUSE_HDFS_NAME_NODE$JOB_CACHE_DIR
   else
      echo "Creating warehouse job cache directory: $WAREHOUSE_HDFS_NAME_NODE$JOB_CACHE_DIR"
   fi
   # copyFromLocal needs the parent directory chain to exist, so ensure that is in place before doing a multi-threaded copyFromLocal
   $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -conf $WAREHOUSE_HADOOP_CONF/hdfs-site.xml -fs $WAREHOUSE_HDFS_NAME_NODE -mkdir -p $WAREHOUSE_HDFS_NAME_NODE${JOB_CACHE_DIR}/..
   $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -conf $WAREHOUSE_HADOOP_CONF/hdfs-site.xml -fs $WAREHOUSE_HDFS_NAME_NODE -copyFromLocal -t $CPUS ${tmpdir} $WAREHOUSE_HDFS_NAME_NODE${JOB_CACHE_DIR}
   # Only do setrep for an hdfs filesystem. Others, such as local or abfs, don't support or need the replication to be set.
   [[ "$WAREHOUSE_HDFS_NAME_NODE" == "hdfs://"* ]] && $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -conf $WAREHOUSE_HADOOP_CONF/hdfs-site.xml -setrep -R ${JOB_CACHE_REPLICATION} $WAREHOUSE_HDFS_NAME_NODE${JOB_CACHE_DIR}
else
   echo "Warehouse and ingest are one in the same. Assuming the warehouse job cache loading is sufficient"
fi

# Remove the prepared directory
rm -r -f $tmpdir
trap - INT TERM EXIT
date


#######################################################################################

# If we made it here, everything is loaded into the new job cache
# directory.  So, just swap the the environment script with the new
# one that will tell jobs to run with the new job cache dir.
cp job-cache-env.sh job-cache-env.bak
mv job-cache-env.tmp job-cache-env.sh
