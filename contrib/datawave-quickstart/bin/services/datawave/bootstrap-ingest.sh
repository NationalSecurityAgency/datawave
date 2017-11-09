# Sourced by env.sh

DW_DATAWAVE_INGEST_SYMLINK="datawave-ingest"

DW_DATAWAVE_INGEST_HOME="${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}"

DW_DATAWAVE_INGEST_LOG_DIR="${DW_DATAWAVE_INGEST_HOME}/logs"

DW_DATAWAVE_INGEST_CONFIG_HOME="${DW_DATAWAVE_INGEST_HOME}/config"

DW_DATAWAVE_INGEST_PASSWD_FILE="${DW_DATAWAVE_INGEST_CONFIG_HOME}"/ingest-passwd.sh

DW_DATAWAVE_INGEST_FLAGFILE_DIR="${DW_DATAWAVE_DATA_DIR}/flags"

DW_DATAWAVE_INGEST_LOCKFILE_DIR="${DW_DATAWAVE_DATA_DIR}/ingest-lock-files"

DW_DATAWAVE_INGEST_HDFS_BASEDIR="/Ingest"

DW_DATAWAVE_INGEST_TEST_FILE_WIKI="${DW_DATAWAVE_SOURCE_DIR}/warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml"

DW_DATAWAVE_INGEST_TEST_FILE_CSV="${DW_DATAWAVE_SOURCE_DIR}/warehouse/ingest-csv/src/test/resources/input/my.csv"

DW_DATAWAVE_INGEST_FLAGMAKER_CONFIGS="${DW_DATAWAVE_INGEST_CONFIG_HOME}/flag-maker-live.xml"

DW_DATAWAVE_INGEST_FLAGMETRICS_DIR="${DW_DATAWAVE_DATA_DIR}/flagMetrics"

DW_DATAWAVE_INGEST_EDGE_DEFINITIONS="config/edge-definitions.xml"

DW_DATAWAVE_INGEST_BULK_DATA_TYPES=""

DW_DATAWAVE_INGEST_LIVE_DATA_TYPES="wikipedia,mycsv"

DW_DATAWAVE_INGEST_NUM_SHARDS=3

# Adding new '-ingestMetricsDisabled' flag to ingest opts here, so that ingest jobs performed
# on a standalone dev instance without the benefit of hadoop native libs can terminate
# cleanly, ie, without this error:
#      Exception in thread "main" java.lang.IllegalArgumentException: SequenceFile doesn't work with GzipCodec without native-hadoop code!
#	     at org.apache.hadoop.io.SequenceFile$Writer.<init>(SequenceFile.java:1088)
#	     at org.apache.hadoop.io.SequenceFile$BlockCompressWriter.<init>(SequenceFile.java:1442)
#	     at org.apache.hadoop.io.SequenceFile.createWriter(SequenceFile.java:275)
#	     at datawave.ingest.mapreduce.job.IngestJob.writeStats(IngestJob.java:1558)
DW_DATAWAVE_MAPRED_INGEST_OPTS="-useInlineCombiner -ingestMetricsDisabled"

DW_DATAWAVE_INGEST_PASSWD_SCRIPT="
export PASSWORD="${DW_ACCUMULO_PASSWORD}"
export TRUSTSTORE_PASSWORD="${DW_ACCUMULO_PASSWORD}"
export KEYSTORE_PASSWORD="${DW_ACCUMULO_PASSWORD}"
"

getDataWaveTarball "${DW_DATAWAVE_INGEST_TARBALL}"
DW_DATAWAVE_INGEST_DIST="${tarball}"
DW_DATAWAVE_INGEST_VERSION="$( echo "${DW_DATAWAVE_INGEST_DIST}" | sed "s/.*\///" | sed "s/datawave-${DW_DATAWAVE_BUILD_PROFILE}-//" | sed "s/-dist.tar.gz//" )"
DW_DATAWAVE_INGEST_BASEDIR="datawave-ingest-${DW_DATAWAVE_INGEST_VERSION}"

# Service helpers...

DW_DATAWAVE_INGEST_CMD_START="( cd ${DW_DATAWAVE_INGEST_HOME}/bin/system && ./start-all.sh -allforce )"
DW_DATAWAVE_INGEST_CMD_STOP="( cd ${DW_DATAWAVE_INGEST_HOME}/bin/system && ./stop-all.sh )"
DW_DATAWAVE_INGEST_CMD_FIND_ALL_PIDS="pgrep -f 'ingest-server.sh|Dapp=FlagMaker|mapreduce.job.BulkIngestMapFileLoader|datawave.ingest.mapreduce.job.IngestJob|ingest/cleanup-server.py'"

function datawaveIngestIsRunning() {
    DW_DATAWAVE_INGEST_PID_LIST="$(eval "${DW_DATAWAVE_INGEST_CMD_FIND_ALL_PIDS} -d ' '")"
    [[ -z "${DW_DATAWAVE_INGEST_PID_LIST}" ]] && return 1 || return 0
}

function datawaveIngestStart() {
    ! hadoopIsRunning && hadoopStart
    ! accumuloIsRunning && accumuloStart

    datawaveIngestIsRunning && echo "DataWave Ingest is already running" || eval "${DW_DATAWAVE_INGEST_CMD_START}"
}

function datawaveIngestStop() {
    datawaveIngestIsRunning && eval "${DW_DATAWAVE_INGEST_CMD_STOP}" || echo "DataWave Ingest is already stopped"
}

function datawaveIngestStatus() {
    datawaveIngestIsRunning && echo "DataWave Ingest is running. PIDs: ${DW_DATAWAVE_INGEST_PID_LIST}" || echo "DataWave Ingest is not running"
}

function datawaveIngestIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}" ] && return 0
    [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_BASEDIR}" ] && return 0
    return 1
}

function datawaveIngestUninstall() {
   if datawaveIngestIsInstalled ; then
      if [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}" ] ; then
          unlink "${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}" || error "Failed to remove DataWave Ingest symlink"
      fi

      if [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_BASEDIR}" ] ; then
          rm -rf "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_BASEDIR}"
      fi

      ! datawaveIngestIsInstalled && info "DataWave Ingest uninstalled" || error "Failed to uninstall DataWave Ingest"
   else
      info "DataWave Ingest not installed. Nothing to do"
   fi
}

function datawaveIngestInstall() {
   "${DW_DATAWAVE_SERVICE_DIR}"/install-ingest.sh
}

function datawaveIngestLoadJobCache() {
    echo
    info "Initializing M/R job cache in HDFS..."
    info "This may take a minute or two, please be patient..."
    ! eval "( cd ${DW_DATAWAVE_INGEST_HOME}/bin/ingest && ./load-job-cache.sh )" && error "Failed to load job cache" && return 1
    echo
}

function datawaveIngestWikipedia() {

   local wikipediaRawFile="${1}"
   local extraOpts="${2}"

   # Here we launch an ingest M/R job directly, via 'bin/ingest/live-ingest.sh', so that we don't have to
   # rely on the DataWave flag maker and other processes to kick it off for us. Thus, the InputFormat class and
   # other options, which are typically configured via the flag maker config (see flag-maker-live.xml)
   # and others, are hardcoded below.

   # Alternatively, to accomplish the same thing, you could start up DataWave Ingest with 'datawaveIngestStart'
   # and simply write the raw file(s) to '${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/wikipedia'. However, you'd have to
   # wait around for the flag maker to pick up and process the raw file(s).

   # Moreover, we use 'live-ingest.sh' here because it offers the least amount of latency, in terms of the time it
   # takes to make the data available for queries. It instructs our 'IngestJob' class to execute a map-only job, which
   # causes key/value mutations to be written directly to our Accumulo tables during the map phase. In contrast,
   # 'bulk-ingest.sh' could be used to generate RFiles instead, but we'd then need our bulk import process to be up
   # and running to load the data into Accumulo.

   [ -z "${wikipediaRawFile}" ] && error "Missing raw file argument" && return 1
   [ ! -f "${wikipediaRawFile}" ] && error "File not found: ${wikipediaRawFile}" && return 1

   local wikipediaHdfsDir="${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/wikipedia"
   local wikipediaHdfsFile="${wikipediaHdfsDir}/$( basename ${wikipediaRawFile} )"
   local putFileCommand="hdfs dfs -copyFromLocal ${wikipediaRawFile} ${wikipediaHdfsDir}"

   local inputFormat="datawave.ingest.wikipedia.WikipediaEventInputFormat"
   local jobCommand="${DW_DATAWAVE_INGEST_HOME}/bin/ingest/live-ingest.sh ${wikipediaHdfsFile} ${DW_DATAWAVE_INGEST_NUM_SHARDS} -inputFormat ${inputFormat} -data.name.override=wikipedia ${extraOpts}"

   echo
   info "Initiating DataWave Ingest job for '${wikipediaRawFile}'"

   launchIngestJob "${wikipediaRawFile}"
}

function datawaveIngestCsv() {

   # Same as with datawaveIngestWikipedia, we use live-ingest.sh, but this time to ingest some CSV data.
   # Note that the sample file, my.csv, has records that intentionally generate errors to demonstrate
   # ingest into DataWave's 'error*' tables, which may be used to easily discover and troubleshoot 
   # data-related errors that arise during ingest. As a result, this job may terminate with warnings

   local csvRawFile="${1}"
   local extraOpts="${2}"

   [ -z "${csvRawFile}" ] && error "Missing raw file argument" && return 1
   [ ! -f "${csvRawFile}" ] && error "File not found: ${csvRawFile}" && return 1

   local csvHdfsDir="${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/mycsv"
   local csvHdfsFile="${csvHdfsDir}/$( basename ${csvRawFile} )"
   local putFileCommand="hdfs dfs -copyFromLocal ${csvRawFile} ${csvHdfsDir}"

   local inputFormat="datawave.ingest.csv.mr.input.CSVFileInputFormat"
   local jobCommand="${DW_DATAWAVE_INGEST_HOME}/bin/ingest/live-ingest.sh ${csvHdfsFile} ${DW_DATAWAVE_INGEST_NUM_SHARDS} -inputFormat ${inputFormat} -data.name.override=mycsv ${extraOpts}"

   launchIngestJob "${csvRawFile}"
}

function launchIngestJob() {

   # Should only be invoked by datawaveIngestCsv, datawaveIngestWikipedia, ...

   echo
   info "Initiating DataWave Ingest job for '${1}'"
   info "Loading raw data into HDFS: '${putFileCommand}'"
   ! eval "${putFileCommand}" && error "Failed to load raw data into HDFS" && return 1
   info "Submitting M/R job: '${jobCommand}'"
   ! eval "${jobCommand}" && warn "Job encountered one or more errors. See job log for details"
   echo
   info "You may view M/R job UI here: http://localhost:8088/cluster"
   echo
}

function datawaveIngestCreateTableSplits() {

   # Creates shard table splits starting on the given date, in YYYYMMDD format (required parameter),
   # and also for the specified number of consecutive days, numDays (required parameter)

   local shardDateYYYYMMDD="${1}"
   local numDays="${2}"
   local shardTables="${3}" # Optional. Comma-delimited 'shard' tables to split, no spaces

   shardTables="${shardTables:-shard}"

   [[ -z "${shardDateYYYYMMDD}" || -z "${numDays}" ]] && error "YYYYMMDD and numDays parameters are required" && return 1

   info "Generating table splits in Accumulo"

   # Create the shard table splits
   ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/create-shards-since.sh ${shardDateYYYYMMDD} ${numDays} "${shardTables}"

   # Go ahead and pre-split the global index and reverse index tables
   ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/seed-index-splits.sh > /dev/null 2>&1

   # Force splits file creation for use by any upcoming jobs, to ensure proper partitioning
   ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/generate-splits-file.sh
}
