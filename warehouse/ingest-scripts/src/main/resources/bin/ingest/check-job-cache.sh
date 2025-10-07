#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
	THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
    MKTEMP="mktemp -t -u $(basename $0)"
else
	THIS_SCRIPT=$(readlink -f $0)
    MKTEMP="mktemp -t $(basename $0).XXXXXXXX"
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ../ingest/ingest-env.sh
. ../ingest/job-cache-env.sh
. ../ingest/ingest-libs.sh

echo "Checking the consistency of $JOB_CACHE_DIR"

# list the hdfs job cache
hdfs_listing=$($MKTEMP)
trap 'rm -f "$hdfs_listing"; exit 1' INT TERM EXIT

$INGEST_HADOOP_HOME/bin/hadoop fs -fs $INGEST_HDFS_NAME_NODE -ls -R $INGEST_HDFS_NAME_NODE$JOB_CACHE_DIR > $hdfs_listing

# check the datawave libraries in the job cache for consistency with the libraries on disk
for c in ${CLASSPATH//:/ }; do
    for f in $(find -L $c -type f); do

	dir=${c%/}
	bname=${dir/*\//}
	fname=${f/*\//}

	# Skip dot files (e.g., config/.edge-key-version.txt.crc)
	[[ "$fname" == "."* ]] && continue

	if [[ "$bname" == "$fname" ]]; then
	    name=$fname
	else
	    name=$bname/${f/*\/$bname\//}
	fi

        # get the local size
	local_file=$(/bin/ls -Ll $f)
	if [[ $? != 0 ]]; then
	    echo "Cannot find $f"
	    exit 1
	fi
	local_size=$(echo $local_file | awk '{print $5}')

        # get the size in hadoop
	hdfs_file=$(grep -F /$name $hdfs_listing)
	hdfs_size=$(echo $hdfs_file | awk '{print $5}')

	if [[ "$local_size" != "$hdfs_size" ]]; then
	    echo "$JOB_CACHE_DIR inconsistent for $name: $local_size != $hdfs_size"
	    exit 1
	fi
    done
done

# If the edge key version file was not created or empty then we want to retry updating it so we fail the check so that
# the load-job-cache.sh gets run which will update the edge key version file
name="edge-key-version.txt"
hdfs_file=$(grep -F /$name $hdfs_listing)
hdfs_size=$(echo $hdfs_file | awk '{print $5}')
if [ "$hdfs_size" == "" ]; then
	echo "$JOB_CACHE_DIR missing $name"
	exit 1
elif [ "$hdfs_size" == "0" ]; then
	echo "$name in $JOB_CACHE_DIR is empty"
	exit 1
fi

rm $hdfs_listing
trap - INT TERM EXIT

echo "$JOB_CACHE_DIR appears consistent"
