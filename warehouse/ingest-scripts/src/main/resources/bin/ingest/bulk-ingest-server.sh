#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ./ingest-env.sh
BIN_DIR=$1
FLAG_DIR=$2
LOG_DIR=$3
MAP_LOADER_INDEX=0

declare -i PIPELINE_COUNT=$((INGEST_BULK_JOBS))

echo "Starting loop to run ingest."

shopt -s extglob
while [ 1 == 1 ]; do

  # first lets startup any markers we have within the pipeline count that we have
  declare -i MARKER_COUNT=$(shopt -s extglob; find ${FLAG_DIR} -regextype posix-egrep -regex ".*_(bulk)_.*\.flag\..*\.marker" 2>/dev/null | wc -l)
  echo "Found $MARKER_COUNT marker files"
  if [[ $((MARKER_COUNT)) -gt 0 ]]; then
    for f in $(shopt -s extglob; find ${FLAG_DIR} -regextype posix-egrep -regex ".*_(bulk)_.*\.flag\..*\.marker" 2>/dev/null); do
      # extract the pipline id
      declare -i PIPELINE=$(flagPipeline $f)
      if [[ $((PIPELINE)) -gt 0 && $((PIPELINE)) -le $((PIPELINE_COUNT)) ]]; then
        echo "$(date) Executing job for marker file $f in pipeline $PIPELINE"

        inprogress_file=$(flagBasename $f).flag.inprogress
        mv $f $inprogress_file
        MAP_LOADER_INDEX=$(( MAP_LOADER_INDEX + 1 ))
        if (( MAP_LOADER_INDEX >= ${#MAP_LOADER_HDFS_NAME_NODES[@]} )); then
            MAP_LOADER_INDEX=0
        fi
        EXTRA_ARGS="-destHdfs ${MAP_LOADER_HDFS_NAME_NODES[$MAP_LOADER_INDEX]}"
        $BIN_DIR/bulk-execute.sh $BIN_DIR $inprogress_file $LOG_DIR $FLAG_DIR -pipelineId $PIPELINE $EXTRA_ARGS &
      else
        echo "$(date) Resetting marker file for ingest pipeline $PIPELINE"
        mv $f $(flagBasename $f).flag
      fi
    done
  fi

  # now make sure we have a flag started for every pipeline
  for (( pipeline=1; pipeline <= $((PIPELINE_COUNT)); pipeline=$((pipeline+1)) )); do
    declare -i PIPELINE_JOB_COUNT=$(ps -ef | grep bulk-execute.sh | grep " -pipelineId ${pipeline} " | wc -l)
    declare -i PIPELINE_MARKER_COUNT=$(shopt -s extglob; find ${FLAG_DIR}/ -regextype posix-egrep -regex ".*_(bulk)_.*\.flag\.${pipeline}\.marker" 2>/dev/null | wc -l)
    declare -i PIPELINE_TOTAL_COUNT=$((PIPELINE_JOB_COUNT + PIPELINE_MARKER_COUNT))
    echo "Found $PIPELINE_TOTAL_COUNT jobs for pipeline $pipeline"
    if [[ $((PIPELINE_TOTAL_COUNT)) == 0 ]]; then
      if [[ "$MAPRED_INGEST_OPTS" =~ "-markerFileLIFO" ]]; then
        flag_files=$(find ${FLAG_DIR}/ -regextype posix-egrep -regex ".*_(bulk)_.*\.flag" -printf "%AY%Aj%AT %p\n" | sort -r | head -1 | awk '{print $2}')
      else
        flag_files=$(find ${FLAG_DIR}/ -regextype posix-egrep -regex ".*_(bulk)_.*\.flag" -printf "%AY%Aj%AT %p\n" | sort | head -1 | awk '{print $2}')
      fi
      for first_flag_file in $flag_files; do
        if [[ -a $first_flag_file ]]; then
            echo "$(date) Executing job for $first_flag_file in pipeline $pipeline"
            mv $first_flag_file ${first_flag_file}.inprogress
            MAP_LOADER_INDEX=$(( MAP_LOADER_INDEX + 1 ))
            if (( $MAP_LOADER_INDEX >= ${#MAP_LOADER_HDFS_NAME_NODES[@]} )); then
                MAP_LOADER_INDEX=0
            fi
            EXTRA_ARGS="-destHdfs ${MAP_LOADER_HDFS_NAME_NODES[$MAP_LOADER_INDEX]}"
            $BIN_DIR/bulk-execute.sh $BIN_DIR ${first_flag_file}.inprogress $LOG_DIR $FLAG_DIR -pipelineId $pipeline $EXTRA_ARGS &
        fi
      done
    fi
  done

  sleep 60
done
