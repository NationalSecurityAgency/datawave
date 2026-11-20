#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
	READLINK_CMD="python -c 'import os,sys;print os.path.realpath(sys.argv[1])'"
	MKTEMP_OPTS="-t $0"
else
	READLINK_CMD="readlink -f"
	MKTEMP_OPTS=""
fi
THIS_SCRIPT=$(eval $READLINK_CMD $0)
THIS_DIR="${THIS_SCRIPT%/*}"

. "$THIS_DIR/ingest-env.sh"
. "$THIS_DIR/ingest-libs.sh"
. "$THIS_DIR/job-cache-env.sh"

# Check that there are no other instances of this script running
acquire_lock_file $(basename "$0") || exit 1

# Read from the DataWave metadata table to create the edge-key version file. Generate it
# in a temporary directory so a failed or empty update cannot replace the last good copy.
EDGE_KEY_CACHE_DIR="$THIS_DIR/../../config"
EDGE_KEY_CACHE_FILE="$EDGE_KEY_CACHE_DIR/edge-key-version.txt"
EDGE_KEY_CACHE_TMP_DIR=$(mktemp -d "$EDGE_KEY_CACHE_DIR/.edge-key-cache.XXXXXXXX") || {
    echo "[ERROR] Unable to create a temporary directory for $EDGE_KEY_CACHE_FILE"
    exit 1
}

if ! "$THIS_DIR/create-edgekey-version-cache.sh" --update "$EDGE_KEY_CACHE_TMP_DIR"; then
    echo "[ERROR] create-edgekey-version-cache.sh failed while generating $EDGE_KEY_CACHE_FILE"
    rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"
    exit 1
fi

if [[ ! -s "$EDGE_KEY_CACHE_TMP_DIR/edge-key-version.txt" ]]; then
    echo "[ERROR] create-edgekey-version-cache.sh did not generate a nonempty $EDGE_KEY_CACHE_FILE"
    rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"
    exit 1
fi

if ! mv "$EDGE_KEY_CACHE_TMP_DIR/edge-key-version.txt" "$EDGE_KEY_CACHE_FILE"; then
    echo "[ERROR] Unable to install the generated edge-key cache at $EDGE_KEY_CACHE_FILE"
    rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"
    exit 1
fi
rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"

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

sed "s%${BEFORE}%${AFTER}%" "$THIS_DIR/job-cache-env.sh" > "$THIS_DIR/job-cache-env.tmp"

. "$THIS_DIR/ingest-libs.sh"

date

# prepare a directory with links to all of the files/directories to put into the jobcache
tmpdir=$(mktemp -d $MKTEMP_OPTS)
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
if [[ $(uname) == "Darwin" ]]; then
	declare -i CPUS=$(sysctl machdep.cpu.thread_count | awk '{print $2}')
else
	declare -i CPUS=$(cat /proc/cpuinfo | grep processor | awk '{print $3}' | sort -n | tail -1)
fi
# lets use twice the number of processors
CPUS=$(echo "$LOAD_JOBCACHE_CPU_MULTIPLIER * $CPUS" | bc)

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

# Update Zookeeper if we have an active job cache path
if [[ -n "${ACTIVE_JOB_CACHE_PATH}" ]]; then
  if ! java -cp ${CLASSPATH} datawave.ingest.jobcache.SetActiveCommand \
    --zookeepers ${INGEST_ZOOKEEPERS} \
    --path ${ACTIVE_JOB_CACHE_PATH} \
    --job-cache "${INGEST_HDFS_NAME_NODE}${JOB_CACHE_DIR}"; then
      echo "[ERROR] Failed to set active ingest job cache"
  fi

  if [[ "$WAREHOUSE_HDFS_NAME_NODE" != "$INGEST_HDFS_NAME_NODE" ]]; then
    if ! java -cp ${CLASSPATH} datawave.ingest.jobcache.SetActiveCommand \
      --zookeepers ${WAREHOUSE_ZOOKEEPERS} \
      --path ${ACTIVE_JOB_CACHE_PATH} \
      --job-cache "${WAREHOUSE_HDFS_NAME_NODE}${JOB_CACHE_DIR}"; then
        echo "[ERROR] Failed to set active warehouse job cache"
    fi
  fi
fi

# Remove the prepared directory
rm -r -f $tmpdir
trap - INT TERM EXIT
date


#######################################################################################

# If we made it here, everything is loaded into the new job cache
# directory.  So, just swap the the environment script with the new
# one that will tell jobs to run with the new job cache dir.
cp "$THIS_DIR/job-cache-env.sh" "$THIS_DIR/job-cache-env.bak"
mv "$THIS_DIR/job-cache-env.tmp" "$THIS_DIR/job-cache-env.sh"
