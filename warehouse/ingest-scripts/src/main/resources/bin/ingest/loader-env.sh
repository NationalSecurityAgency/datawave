#!/bin/bash

# If replacement property MAP_LOADER_HDFS_NAME_NODES_CONFIG is non-empty, override legacy properties:
# MAP_LOADER_HDFS_NAME_NODES and NUM_MAP_LOADERS
if [[ ! -z ${MAP_LOADER_HDFS_NAME_NODES_CONFIG} ]]; then
  OLD_IFS="${IFS}"
  IFS=";" MAP_LOADER_HDFS_NAME_NODES=($MAP_LOADER_HDFS_NAME_NODES_CONFIG)
  MAP_LOADER_HDFS_DIRS=($MAP_LOADER_HDFS_NAME_NODES_CONFIG)
  INDEX=0

  for MAP_LOADER_HDFS_NAME_NODE in ${MAP_LOADER_HDFS_NAME_NODES[@]}; do
    # Number of loaders for current entry
    NUM_MAP_LOADERS[$INDEX]=$(echo $MAP_LOADER_HDFS_NAME_NODE | cut -d, -f2)

    # Namenode (without directory) for current entry
    MAP_LOADER_HDFS_NAME_NODES[$INDEX]=$(echo $MAP_LOADER_HDFS_NAME_NODE | cut -d, -f1 | cut -d/ -f1-3)

    # For viewfs, add the last / back on
    case $MAP_LOADER_HDFS_NAME_NODES[$INDEX] in viewfs:*)
      MAP_LOADER_HDFS_NAME_NODES[$INDEX]="${MAP_LOADER_HDFS_NAME_NODES[$INDEX]}/"
    esac

    # Directory (without namenode) for current entry.  If undefined, use base work directory.
    MAP_LOADER_HDFS_DIRS[$INDEX]=$(echo $MAP_LOADER_HDFS_NAME_NODE | cut -d, -f1 | cut -d/ -f4- | sed 's/^/\//')
    if [[ "${MAP_LOADER_HDFS_DIRS[$INDEX]}" == "/" ]]; then
      MAP_LOADER_HDFS_DIRS[$INDEX]=${BASE_WORK_DIR}
    fi

    # Insist on numerical format for NUM_MAP_LOADERS entries
    if ! [[ ${NUM_MAP_LOADERS[INDEX]} =~ '^[0-9]+$' ]]; then
      echo "Invalid format for MAP_LOADER_HDFS_NAME_NODES_CONFIG."
      echo "Example configuration: MAP_LOADER_HDFS_NAME_NODES_CONFIG=hdfs://namenode-1,1;hdfs://namenode-2,3;"
      echo "Exiting..."
      exit -1
    fi

    INDEX=$((INDEX+1))
  done

#  export MAP_LOADER_HDFS_NAME_NODES
#  export MAP_LOADER_HDFS_DIRS
#  export NUM_MAP_LOADERS
  IFS="${OLD_IFS}"
else
  OLD_IFS="${IFS}"
  MAP_LOADER_HDFS_NAME_NODES=( $MAP_LOADER_HDFS_NAME_NODES )
  NUM_MAP_LOADERS=( $NUM_MAP_LOADERS )
  IFS="${OLD_IFS}"
fi

EXPECTED_LOADER_COUNT=0
for (( LOADER=0; LOADER < ${#NUM_MAP_LOADERS[@]}; LOADER=$((LOADER + 1)) )); do
  EXPECTED_LOADER_COUNT=$((EXPECTED_LOADER_COUNT+NUM_MAP_LOADERS[$LOADER]))
done
if [[ ! -z ${EXTRA_MAP_LOADER} ]]; then
  EXPECTED_LOADER_COUNT=$((EXPECTED_LOADER_COUNT+1))
fi
function compare () {
  ACTUAL_COUNT=$($MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader" -c)
}
