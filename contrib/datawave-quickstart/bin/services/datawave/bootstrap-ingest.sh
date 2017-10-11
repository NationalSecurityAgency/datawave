# Sourced by env.sh

DW_DATAWAVE_INGEST_SYMLINK="datawave-ingest"
DW_DATAWAVE_INGEST_HOME="${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}"

DW_DATAWAVE_INGEST_LOG_DIR="${DW_DATAWAVE_INGEST_HOME}/logs"
DW_DATAWAVE_INGEST_CONFIG_HOME="${DW_DATAWAVE_INGEST_HOME}/config"
DW_DATAWAVE_INGEST_PASSWD_FILE="${DW_DATAWAVE_INGEST_CONFIG_HOME}"/ingest-passwd.sh

DW_DATAWAVE_INGEST_FLAGFILE_DIR="${DW_DATAWAVE_DATA_DIR}/flags"

DW_DATAWAVE_INGEST_LOCKFILE_DIR="${DW_DATAWAVE_DATA_DIR}/ingest-lock-files"

DW_DATAWAVE_INGEST_HDFS_BASEDIR="/Ingest"

DW_DATAWAVE_INGEST_HDFS_INPUTS="${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/wikipedia"

DW_DATAWAVE_INGEST_TEST_FILE="${DW_DATAWAVE_SOURCE_DIR}/warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml"

DW_DATAWAVE_INGEST_FLAGMAKER_CONFIGS="${DW_DATAWAVE_INGEST_CONFIG_HOME}/WikipediaFlagMakerConfig.xml"
DW_DATAWAVE_INGEST_FLAGMETRICS_DIR="${DW_DATAWAVE_DATA_DIR}/flagMetrics"
DW_DATAWAVE_INGEST_BULK_DATA_TYPES="mycsv"
DW_DATAWAVE_INGEST_LIVE_DATA_TYPES="wikipedia"

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
export CACHE_PWORD="${DW_ACCUMULO_PASSWORD}"
export TRUSTSTORE_PASSWORD="${DW_ACCUMULO_PASSWORD}"
export KEYSTORE_PASSWORD="${DW_ACCUMULO_PASSWORD}"
export AGEOFF_SERVER_CERT_PASS="${DW_ACCUMULO_PASSWORD}"
"

# Accumulo Shell script for initializing whatever we need for DataWave
DW_ACCUMULO_SHELL_INIT_SCRIPT="
config -s table.classpath.context=datawave
createtable QueryMetrics_m
setauths -s PUBLIC,PRIVATE
quit
"

getDataWaveTarball "${DW_DATAWAVE_INGEST_TARBALL}"
DW_DATAWAVE_INGEST_DIST="${tarball}"
DW_DATAWAVE_INGEST_VERSION="$( echo "${DW_DATAWAVE_INGEST_DIST}" | sed "s/.*\///" | sed "s/datawave-dev-//" | sed "s/-dist.tar.gz//" )"
DW_DATAWAVE_INGEST_BASEDIR="datawave-ingest-${DW_DATAWAVE_INGEST_VERSION}"

# Service helpers...

DW_DATAWAVE_INGEST_CMD_START="( cd ${DW_DATAWAVE_INGEST_HOME}/bin/system && ./start-all.sh -allforce )"
DW_DATAWAVE_INGEST_CMD_STOP="( cd ${DW_DATAWAVE_INGEST_HOME}/bin/system && ./stop-all.sh )"
DW_DATAWAVE_INGEST_CMD_FIND_ALL_PIDS="pgrep -f 'ingest-server.sh|Dapp=FlagMaker|mapreduce.job.BulkIngestMapFileLoader|datawave.ingest.mapreduce.job.IngestJob|ingest/cleanupserver.py'"

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

   local wikipediaRawFile="$1"
   local extraOpts="$2"

   [ -z "${wikipediaRawFile}" ] && error "Missing raw file argument" && return 1
   [ ! -f "${wikipediaRawFile}" ] && error "File not found: ${wikipediaRawFile}" && return 1

   local wikipediaHdfsDir="/Ingest/wikipedia"
   local wikipediaHdfsFile="${wikipediaHdfsDir}/$( basename ${wikipediaRawFile} )"
   local putFileCommand="hdfs dfs -copyFromLocal ${wikipediaRawFile} ${wikipediaHdfsDir}"

   local numShards=10 # Should match whatever is currently configured for NUM_SHARDS (in dev.properties)
   local inputFormat="datawave.ingest.wikipedia.WikipediaEventInputFormat"
   local jobCommand="${DW_DATAWAVE_INGEST_HOME}/bin/ingest/live-ingest.sh ${wikipediaHdfsFile} ${numShards} -inputFormat ${inputFormat} ${extraOpts}"

   echo
   info "Initiating DataWave Ingest job for '${wikipediaRawFile}'"
   info "Loading raw data into HDFS: '${putFileCommand}'"
   ! eval "${putFileCommand}" && error "Failed to load raw data into HDFS" && return 1
   info "Submitting M/R job: '${jobCommand}'"
   sleep 3
   ! eval "${jobCommand}" && error "Job failed" && return 1
   echo
   info "You may view M/R job UI here: http://localhost:8088/cluster"
   echo
}

function datawaveIngestCreateTables() {
    #
    # Run a "create tables" ingest job (ie, a typical job, but with '-createTablesOnly' flag)
    # to ensure that all Accumulo tables are created & configured
    #
    local wikipediaRawFileBasename="$( basename ${DW_DATAWAVE_INGEST_TEST_FILE} )"
    local wikipediaRawFile="/tmp/${wikipediaRawFileBasename}.create_tables.$(date +%Y-%m-%d-%H%M%S)"
    cp "${DW_DATAWAVE_INGEST_TEST_FILE}" "${wikipediaRawFile}"

    ! datawaveIngestWikipedia "${wikipediaRawFile}" "-createTablesOnly" && error "Table creation job encountered problems"

    # Cleanup...
    rm -f "${wikipediaRawFile}"
    local wikipediaHdfsFile="/Ingest/wikipedia/$( basename ${wikipediaRawFile} )"
    hdfs dfs -rm "${wikipediaHdfsFile}"
}