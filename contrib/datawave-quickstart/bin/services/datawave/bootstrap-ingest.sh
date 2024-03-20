# Sourced by env.sh

DW_DATAWAVE_INGEST_SYMLINK="datawave-ingest"
DW_DATAWAVE_INGEST_BASEDIR="datawave-ingest-install"

DW_DATAWAVE_INGEST_HOME="${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}"

# Here we define number of desired 'shards', which controls the # of shard table splits in Accumulo and also the # of bulk
# ingest reducers. Set to 1 for standalone instance, but typically set to the first prime number that is less than the
# number of available Accumulo tablet servers...

DW_DATAWAVE_INGEST_NUM_SHARDS=${DW_DATAWAVE_INGEST_NUM_SHARDS:-1}

# Ingest job logs, etc

DW_DATAWAVE_INGEST_LOG_DIR="${DW_DATAWAVE_INGEST_HOME}/logs"

# Ingest configs

DW_DATAWAVE_INGEST_CONFIG_HOME="${DW_DATAWAVE_INGEST_HOME}/config"

# DataWave ingest utilizes a simple password script file to provide any passwords that
# may be required by various components, allowing for runtime configuration of any necessary
# passwords. See datawave-ingest/bin/ingest/ingest-env.sh

DW_DATAWAVE_INGEST_PASSWD_FILE="${DW_DATAWAVE_INGEST_CONFIG_HOME}"/ingest-passwd.sh

# Password script definition...

DW_DATAWAVE_INGEST_PASSWD_SCRIPT="
export PASSWORD="${DW_ACCUMULO_PASSWORD}"
export TRUSTSTORE_PASSWORD="${DW_DATAWAVE_TRUSTSTORE_PASSWORD}"
export KEYSTORE_PASSWORD="${DW_DATAWAVE_KEYSTORE_PASSWORD}"
"

# Directory to persist *.flag files, when automating ingest job processing via the FlagMaker process

DW_DATAWAVE_INGEST_FLAGFILE_DIR="${DW_DATAWAVE_DATA_DIR}/flags"

# Comma-delimited list of configs for the FlagMaker process(es)

DW_DATAWAVE_INGEST_FLAGMAKER_CONFIGS=${DW_DATAWAVE_INGEST_FLAGMAKER_CONFIGS:-"${DW_DATAWAVE_INGEST_CONFIG_HOME}/flag-maker-live.xml"}

# Dir for ingest-related 'pid' files

DW_DATAWAVE_INGEST_LOCKFILE_DIR="${DW_DATAWAVE_DATA_DIR}/ingest-lock-files"

# Base HDFS dir for ingest

DW_DATAWAVE_INGEST_HDFS_BASEDIR=${DW_DATAWAVE_INGEST_HDFS_BASEDIR:-/datawave/ingest}

# Set to any non-empty value other than 'false' to skip ingest of the raw data examples below

DW_DATAWAVE_INGEST_TEST_SKIP=${DW_DATAWAVE_INGEST_TEST_SKIP:-false}

# Example raw data files to be ingested (unless DW_DATAWAVE_INGEST_TEST_SKIP != 'false')

DW_DATAWAVE_INGEST_TEST_FILE_WIKI=${DW_DATAWAVE_INGEST_TEST_FILE_WIKI:-"${DW_DATAWAVE_SOURCE_DIR}/warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml"}
DW_DATAWAVE_INGEST_TEST_FILE_CSV=${DW_DATAWAVE_INGEST_TEST_FILE_CSV:-"${DW_DATAWAVE_SOURCE_DIR}/warehouse/ingest-csv/src/test/resources/input/my.csv"}
DW_DATAWAVE_INGEST_TEST_FILE_JSON=${DW_DATAWAVE_INGEST_TEST_FILE_JSON:-"${DW_DATAWAVE_SOURCE_DIR}/warehouse/ingest-json/src/test/resources/input/tvmaze-api.json"}

DW_DATAWAVE_INGEST_FLAGMETRICS_DIR="${DW_DATAWAVE_DATA_DIR}/flagMetrics"

# Spring config defining edges to be created during ingest, for building distributed graph(s) in DW's edge table
# (For now, this file path must be relative to the ingest home directory)

DW_DATAWAVE_INGEST_EDGE_DEFINITIONS=${DW_DATAWAVE_INGEST_EDGE_DEFINITIONS:-"config/edge-definitions.xml"}

# Comma-delimited data type identifiers to be ingested via "live" ingest, ie via low-latency batch mutations into Accumulo tables

DW_DATAWAVE_INGEST_LIVE_DATA_TYPES=${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES:-"wikipedia,mycsv,myjson"}

# Comma-delimited data type identifiers to be ingested via "bulk" ingest, ie via bulk import of RFiles into Accumulo tables

DW_DATAWAVE_INGEST_BULK_DATA_TYPES=${DW_DATAWAVE_INGEST_BULK_DATA_TYPES:-"shardStats"}

DW_DATAWAVE_MAPRED_INGEST_OPTS=${DW_DATAWAVE_MAPRED_INGEST_OPTS:-"-useInlineCombiner -ingestMetricsDisabled"}

getDataWaveTarball "${DW_DATAWAVE_INGEST_TARBALL}"
DW_DATAWAVE_INGEST_DIST="${tarball}"

# Service helpers...

DW_DATAWAVE_INGEST_CMD_START="( cd ${DW_DATAWAVE_INGEST_HOME}/bin/system && ./start-all.sh -allforce )"
DW_DATAWAVE_INGEST_CMD_STOP="( cd ${DW_DATAWAVE_INGEST_HOME}/bin/system && ./stop-all.sh )"
DW_DATAWAVE_INGEST_CMD_FIND_ALL_PIDS="pgrep -d ' ' -f 'ingest-server.sh|Dapp=FlagMaker|mapreduce.job.BulkIngestMapFileLoader|datawave.ingest.mapreduce.job.IngestJob|ingest/cleanup-server.py'"

function datawaveIngestIsRunning() {
    DW_DATAWAVE_INGEST_PID_LIST="$(eval "${DW_DATAWAVE_INGEST_CMD_FIND_ALL_PIDS}")"
    [[ -z "${DW_DATAWAVE_INGEST_PID_LIST}" ]] && return 1 || return 0
}

function datawaveIngestStart() {
    ! hadoopIsRunning && hadoopStart
    ! accumuloIsRunning && accumuloStart

    datawaveIngestIsRunning && echo "DataWave Ingest is already running" || eval "${DW_DATAWAVE_INGEST_CMD_START}"
}

function datawaveIngestStop() {
    datawaveIngestIsRunning && eval "${DW_DATAWAVE_INGEST_CMD_STOP}" || echo "DataWave Ingest is already stopped"
    rm -f "${DW_DATAWAVE_INGEST_LOCKFILE_DIR}"/*.LCK
}

function datawaveIngestStatus() {

    echo "======  DataWave Ingest Status  ======"
    if datawaveIngestIsRunning ; then
        echo "pids: ${DW_DATAWAVE_INGEST_PID_LIST}"
        $DW_DATAWAVE_INGEST_HOME/bin/ingest/list-ingest.sh

    else
        info "No ingest processes are running"
    fi

}

function datawaveIngestIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}" ] && return 0
    [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_BASEDIR}" ] && return 0
    return 1
}

function datawaveIngestUninstall() {

   # Guard against false negative (bug) by testing to see if a "datawave-ingest-*" dir does actually exist, since
   # DW_DATAWAVE_INGEST_BASEDIR value may have been only partially defined (without the DW version, e.g., if build failed, etc)
   # This is only for backward compatibility, since now the dir name is no longer defined dynamically
   local install_dir="$( find "${DW_DATAWAVE_SERVICE_DIR}" -maxdepth 1 -type d -name "${DW_DATAWAVE_INGEST_BASEDIR}*" )"

   if datawaveIngestIsInstalled || [ -n "${install_dir}" ] ; then
      if [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}" ] ; then
          unlink "${DW_CLOUD_HOME}/${DW_DATAWAVE_INGEST_SYMLINK}" || error "Failed to remove DataWave Ingest symlink"
      fi

      if [ -n "${install_dir}" ] ; then
          rm -rf "${install_dir}"
      fi

      ! datawaveIngestIsInstalled && info "DataWave Ingest uninstalled" || error "Failed to uninstall DataWave Ingest"
   else
      info "DataWave Ingest not installed. Nothing to do"
   fi
}

function datawaveIngestInstall() {

   export DW_SKIP_INGEST_EXAMPLES=${DW_SKIP_INGEST_EXAMPLES:-false}

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

   # Uses example ingest config: wikipedia-ingest-config.xml

   local wikipediaRawFile="${1}"
   local extraOpts="${2}"

   # Here we launch an ingest M/R job directly via 'bin/ingest/live-ingest.sh', so that we don't have to
   # rely on the DataWave flag maker and other processes to kick it off for us. Thus, the InputFormat class and
   # other options, which are typically configured via the flag maker config (see flag-maker-live.xml)
   # and others, are hardcoded below.

   # Alternatively, to accomplish the same thing, you could start up DataWave Ingest with 'datawaveIngestStart'
   # and simply write the raw file(s) to '${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/wikipedia'. However, you'd have to
   # wait for the flag maker to pick up and process the raw file(s).

   # Moreover, we use 'live-ingest.sh' here because it offers the least amount of latency, in terms of the time it
   # takes to make the data available for queries. It instructs our 'IngestJob' class to execute a map-only job, which
   # causes key/value mutations to be written directly to our Accumulo tables during the map phase. In contrast,
   # 'bulk-ingest.sh' could be used to generate RFiles instead, but we'd then need our bulk import process to be up
   # and running to load the data into Accumulo.

   [ -z "${wikipediaRawFile}" ] && error "Missing raw file argument" && return 1
   [ ! -f "${wikipediaRawFile}" ] && error "File not found: ${wikipediaRawFile}" && return 1

   local wikipediaHdfsFile="${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/$( basename ${wikipediaRawFile} )"
   local putFileCommand="hdfs dfs -copyFromLocal -f ${wikipediaRawFile} ${wikipediaHdfsFile}"

   local inputFormat="datawave.ingest.wikipedia.WikipediaEventInputFormat"
   local jobCommand="${DW_DATAWAVE_INGEST_HOME}/bin/ingest/live-ingest.sh ${wikipediaHdfsFile} ${DW_DATAWAVE_INGEST_NUM_SHARDS} -inputFormat ${inputFormat} -data.name.override=wikipedia ${extraOpts}"

   echo
   info "Initiating DataWave Ingest job for '${wikipediaRawFile}'"

   launchIngestJob "${wikipediaRawFile}"
}

function datawaveIngestCsv() {

   # Uses example ingest config: mycsv-ingest-config.xml

   # Same as with datawaveIngestWikipedia, we use live-ingest.sh, but this time to ingest some CSV data.
   # Note that the sample file, my.csv, has records that intentionally generate errors to demonstrate
   # ingest into DataWave's 'error*' tables, which may be used to easily discover and troubleshoot
   # data-related errors that arise during ingest. As a result, this job may terminate with warnings

   local csvRawFile="${1}"
   local extraOpts="${2}"

   [ -z "${csvRawFile}" ] && error "Missing raw file argument" && return 1
   [ ! -f "${csvRawFile}" ] && error "File not found: ${csvRawFile}" && return 1

   local csvHdfsFile="${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/$( basename ${csvRawFile} )"
   local putFileCommand="hdfs dfs -copyFromLocal -f ${csvRawFile} ${csvHdfsFile}"

   local inputFormat="datawave.ingest.csv.mr.input.CSVFileInputFormat"
   local jobCommand="${DW_DATAWAVE_INGEST_HOME}/bin/ingest/live-ingest.sh ${csvHdfsFile} ${DW_DATAWAVE_INGEST_NUM_SHARDS} -inputFormat ${inputFormat} -data.name.override=mycsv ${extraOpts}"

   launchIngestJob "${csvRawFile}"
}

function datawaveIngestJson() {

   # Uses example ingest config: myjson-ingest-config.xml

   # Again we use live-ingest.sh, but this time to ingest some JSON data

   local jsonRawFile="${1}"
   local extraOpts="${2}"

   [ -z "${jsonRawFile}" ] && error "Missing raw file argument" && return 1
   [ ! -f "${jsonRawFile}" ] && error "File not found: ${jsonRawFile}" && return 1

   local jsonHdfsFile="${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/$( basename ${jsonRawFile} )"
   local putFileCommand="hdfs dfs -copyFromLocal -f ${jsonRawFile} ${jsonHdfsFile}"

   local inputFormat="datawave.ingest.json.mr.input.JsonInputFormat"
   local jobCommand="${DW_DATAWAVE_INGEST_HOME}/bin/ingest/live-ingest.sh ${jsonHdfsFile} ${DW_DATAWAVE_INGEST_NUM_SHARDS} -inputFormat ${inputFormat} -data.name.override=myjson ${extraOpts}"

   launchIngestJob "${jsonRawFile}"
}

function launchIngestJob() {

   # Should only be invoked by datawaveIngestCsv, datawaveIngestWikipedia, datawaveIngestJson...

   echo
   info "Initiating DataWave Ingest job for '${1}'"
   info "Loading raw data into HDFS: '${putFileCommand}'"
   ! eval "${putFileCommand}" && error "Failed to load raw data into HDFS" && return 1
   info "Submitting M/R job: '${jobCommand}'"

   eval "${jobCommand}"
   DW_LAST_INGEST_JOB_STATUS=$?

   checkForIngestJobErrors

   return ${DW_LAST_INGEST_JOB_STATUS}
}

function checkForIngestJobErrors() {

    # If possible, drop some helpful hints about any errors encountered and where
    # to go for more information regarding the ingest job that just completed

    if [ ${DW_LAST_INGEST_JOB_STATUS} != 0 ] ; then
       warn "The IngestJob class encountered errors (exit status: ${DW_LAST_INGEST_JOB_STATUS}). See job log above for details"
    else
       info "The IngestJob class terminated with normal exit status"
    fi

    echo
    info "You may view M/R job UI here: http://localhost:8088/cluster"
    echo

    if [[ -n "${csvRawFile}" && ${DW_LAST_INGEST_JOB_STATUS} == 251 ]] ; then

       # Job had processing errors. If the CSV ingested was our test file, then display some info on the known errors...

       local testCsvMd5=$( md5sum "${DW_DATAWAVE_INGEST_TEST_FILE_CSV}" | cut -d ' ' -f1 )
       local currentMd5=$( md5sum "${csvRawFile}" | cut -d ' ' -f1 )

       if [ "${testCsvMd5}" == "${currentMd5}" ] ; then
          local TEST_CSV="$( basename "${DW_DATAWAVE_INGEST_TEST_FILE_CSV}" )"
          local MISSING_DATA_ERROR="$( printYellow "MISSING_DATA_ERROR" )"
          local EVENT_FATAL_ERROR="$( printYellow "EVENT_FATAL_ERROR" )"
          local SUCCEEDED="$( printGreen "SUCCEEDED" )"
          local EXAMPLE_POLICY_ENFORCER="$( printGreen "datawave.policy.ExampleIngestPolicyEnforcer" )"
          info "NOTE: Regarding the [$( printYellow "DW-WARN" )] message above and associated errors..."
          echo
          echo "    By design, the test file '${TEST_CSV}' should have generated 1 ${EVENT_FATAL_ERROR} and 1"
          echo "    ${MISSING_DATA_ERROR}, both of which should be reflected in the 'counters' section of the job"
          echo "    log above. Both are due to one 'bad' CSV record having a null SECURITY_MARKING field. For"
          echo "    demonstration purposes, we've forced the missing-data error via the class configured to be"
          echo "    our IngestPolicyEnforcer, which handles record-level validations..."
          echo
          echo "    In 'mycsv-ingest-config.xml', note our policy enforcer: ${EXAMPLE_POLICY_ENFORCER}."
          echo "    As a result, CSV records found to be flagged with ${MISSING_DATA_ERROR} will be evicted and"
          echo "    will not be written to DataWave's primary data schema, i.e., the 'shard*' tables"
          echo
          echo "    Thus, the M/R job's 'FinalApplicationStatus' should still be ${SUCCEEDED}, but as a result"
          echo "    of the errors, you should find that DataWave's 'error*' tables were populated with metadata"
          echo "    from the invalid record. For more details, scan the 'errorShard' table with Accumulo Shell,"
          echo "    or query the error schema with DataWave's Query API (see test-web/tests/ErrorEventQuery.test)"
          echo
       fi
   fi

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

function datawaveIngestDisplayBinaryInfo() {
  echo "Source: DataWave Ingest Version $(getDataWaveVersion)//$(datawaveIngestTarballName)"
  local installedTarball="$(ls -1 ${DW_DATAWAVE_SERVICE_DIR}/$(basename ${DW_DATAWAVE_INGEST_TARBALL}) 2>/dev/null)"
  if [[ -n "${installedTarball}" ]]; then
     echo " Local: ${installedTarball}"
  else
     echo " Local: Not loaded"
  fi
}

function datawaveIngestTarballName() {
   local dwVersion="$(getDataWaveVersion)"
   echo "$( basename "${DW_DATAWAVE_INGEST_TARBALL/-\*-/-$dwVersion-}" )"
}

function datawaveIngestExamples() {
   datawaveIngestWikipedia ${DW_DATAWAVE_INGEST_TEST_FILE_WIKI}
   datawaveIngestJson ${DW_DATAWAVE_INGEST_TEST_FILE_JSON}
   datawaveIngestCsv ${DW_DATAWAVE_INGEST_TEST_FILE_CSV}
}

