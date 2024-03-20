#!/usr/bin/env bash

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"
source "${SERVICES_DIR}/hadoop/bootstrap.sh"
source "${SERVICES_DIR}/accumulo/bootstrap.sh"

[ -z "$( which bc )" ] && fatal "DataWave Ingest install cannot proceed because 'bc' was not found. Please install 'bc' and then resume via 'allInstall' or 'datawaveIngestInstall"

hadoopIsInstalled || fatal "DataWave Ingest requires that Hadoop be installed"
accumuloIsInstalled || fatal "DataWave Ingest requires that Accumulo be installed"

datawaveIngestIsInstalled && info "DataWave Ingest is already installed" && exit 1

[ -f "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_DIST}" ] || fatal "DataWave Ingest tarball not found"

TARBALL_BASE_DIR="${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_BASEDIR}"

# Extract, set symlink, and verify...
mkdir "${TARBALL_BASE_DIR}" || fatal "Failed to create DataWave Ingest base directory"
tar xf "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_INGEST_DIST}" -C "${TARBALL_BASE_DIR}" --strip-components=1 || fatal "Failed to extract DataWave Ingest tarball"
( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/datawave/${DW_DATAWAVE_INGEST_BASEDIR}" "${DW_DATAWAVE_INGEST_SYMLINK}" ) || fatal "Failed to set DataWave Ingest symlink"

! datawaveIngestIsInstalled && fatal "DataWave Ingest was not installed"

info "DataWave Ingest tarball extracted and symlinked"

if ! hadoopIsRunning ; then
   info "Starting Hadoop, so that we can initialize Accumulo"
   hadoopStart
fi

# Create any Hadoop directories related to Datawave Ingest
if [[ -n "${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES}" ]] ; then

   OLD_IFS="${IFS}"
   IFS=","
   HDFS_RAW_INPUT_DIRS=( ${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES} )
   IFS="${OLD_IFS}"

   for dir in "${HDFS_RAW_INPUT_DIRS[@]}" ; do
      hdfs dfs -mkdir -p "${DW_DATAWAVE_INGEST_HDFS_BASEDIR}/${dir}" || fatal "Failed to create HDFS directory: ${dir}"
   done
fi

#----------------------------------------------------------
# Configure/update Accumulo classpath, set auths, etc
#----------------------------------------------------------

if accumuloIsRunning ; then
  info "Stopping Accumulo to update classpath, etc..."
  accumuloStop || warn "Failed to stop an already running Accumulo"
fi

# Update Accumulo classpath for DataWave jars in HDFS/VFS directories (or lib/ext)

if [ "${DW_ACCUMULO_VFS_DATAWAVE_ENABLED}" == true ]; then
   if ${HADOOP_HOME}/bin/hdfs dfs -test -f ${DW_ACCUMULO_VFS_DATAWAVE_DIR}/*.jar > /dev/null 2>&1 ; then
      ${HADOOP_HOME}/bin/hdfs dfs -rm -R ${DW_ACCUMULO_VFS_DATAWAVE_DIR}/* || fatal "HDFS delete failed for ${DW_ACCUMULO_VFS_DATAWAVE_DIR}/*"
   fi
   info "Copying DataWave jars into HDFS dir: ${DW_ACCUMULO_VFS_DATAWAVE_DIR}"
   if [ -d ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib ]; then
      ${HADOOP_HOME}/bin/hdfs dfs -put -f ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/*.jar ${DW_ACCUMULO_VFS_DATAWAVE_DIR}
   fi
   if [ -d ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/ext ]; then
      ${HADOOP_HOME}/bin/hdfs dfs -put -f ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/ext/*.jar ${DW_ACCUMULO_VFS_DATAWAVE_DIR}
   fi
else
   mkdir "${ACCUMULO_HOME}/lib/ext"
   [ ! -d ${ACCUMULO_HOME}/lib/ext ] && fatal "Unable to update Accumulo classpath. ${ACCUMULO_HOME}/lib/ext does not exist!"
   info "Removing any existing jars from ${ACCUMULO_HOME}/lib/ext"
   rm -f ${ACCUMULO_HOME}/lib/ext/*.jar
   info "Copying DataWave jars into ${ACCUMULO_HOME}/lib and ${ACCUMULO_HOME}/lib/ext"
   if [ -d ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib ]; then
      cp ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/*.jar ${ACCUMULO_HOME}/lib > /dev/null 2>&1
   fi
   if [ -d ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/ext ]; then
      cp ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/ext/*.jar ${ACCUMULO_HOME}/lib/ext > /dev/null 2>&1
   fi
fi

# Make sure Accumulo service is started.
if ! accumuloIsRunning ; then
   info "Starting Accumulo, so that we can initialize DataWave Ingest"
   accumuloStart
fi

# Perform additional Accumulo configs dynamically as needed.
createAccumuloShellInitScript
ACCUMULO_TMP_SCRIPT="$(mktemp -t `basename $0`-accumulo-shell.XXXXXX)"
echo "${DW_ACCUMULO_SHELL_INIT_SCRIPT}" > $ACCUMULO_TMP_SCRIPT
info "Executing the following Accumulo shell script: $ACCUMULO_TMP_SCRIPT"
info "${ACCUMULO_TMP_SCRIPT} contents... "
cat "${ACCUMULO_TMP_SCRIPT}"

OK_TO_EXEC_INIT_SCRIPT=true
if [ "${DW_REDEPLOY_IN_PROGRESS}" == true ] ; then
    if ! askYesNo "Do you want to execute the '${ACCUMULO_TMP_SCRIPT}' script above (if desired, modify the file on disk before affirming)?" ; then
        OK_TO_EXEC_INIT_SCRIPT=false
    fi
fi

if [ "${OK_TO_EXEC_INIT_SCRIPT}" == true ] ; then
    ${ACCUMULO_HOME}/bin/accumulo shell -u root -p "${DW_ACCUMULO_PASSWORD}" -f "${ACCUMULO_TMP_SCRIPT}" || fatal "Failed to execute $ACCUMULO_TMP_SCRIPT on Accumulo!"
fi

# ----------------------
# Setup DataWave Ingest
# ----------------------

# Write DW_DATAWAVE_INGEST_PASSWD_SCRIPT
if [ -d ${DW_DATAWAVE_INGEST_CONFIG_HOME} ]; then
   echo "${DW_DATAWAVE_INGEST_PASSWD_SCRIPT}" > ${DW_DATAWAVE_INGEST_PASSWD_FILE}
   info "DataWave Ingest ingest-passwd.sh written"
else
   error "Unable to write ingest-passwd.sh, missing ${DW_DATAWAVE_INGEST_CONFIG_HOME} directory"
fi

[ ! -d "${DW_DATAWAVE_INGEST_LOG_DIR}" ] && assertCreateDir "${DW_DATAWAVE_INGEST_LOG_DIR}"
[ ! -d "${DW_DATAWAVE_INGEST_FLAGFILE_DIR}" ] && assertCreateDir "${DW_DATAWAVE_INGEST_FLAGFILE_DIR}"
[ ! -d "${DW_DATAWAVE_INGEST_LOCKFILE_DIR}" ] && assertCreateDir "${DW_DATAWAVE_INGEST_LOCKFILE_DIR}"

OK_TO_LOAD_JOB_CACHE=true
if [ "${DW_REDEPLOY_IN_PROGRESS}" == true ] ; then
   if ! askYesNo "Do you want to re-load the ingest job cache in HDFS?" ; then
        OK_TO_LOAD_JOB_CACHE=false
   fi
fi

if [ "${OK_TO_LOAD_JOB_CACHE}" == true ] ; then
   ! datawaveIngestLoadJobCache && fatal "DataWave Ingest initialization failed"
fi

#==============================================================================
# Creates the datawave tables and creates splits. Set the environment variable
# DW_DATAWAVE_SKIP_CREATE_TABLES=true to disable creation of datawave tables.
function initializeDatawaveTables() {
    # create tables
    if [[ ${DW_DATAWAVE_SKIP_CREATE_TABLES} != true ]]; then
        info "creating datawave tables, splits, and caches ..."
        ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/create-all-tables.sh || fatal "error creating tables"
        # create splits for shard table for today
        local _today=$(date "+%Y%m%d")
        ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/create-shards-since.sh ${_today}
        # always pre-split non sharded tables
        ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/seed-index-splits.sh > /dev/null 2>&1

        # set splits cache
        ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/generate-splits-file.sh

	#set config cache
	 ${DW_DATAWAVE_INGEST_HOME}/bin/ingest/generate-accumulo-config-cache.sh

    fi
}


initializeDatawaveTables


echo
info "DataWave Ingest initialized and ready to start..."
echo
echo "       Start command: datawaveIngestStart"
echo "        Stop command: datawaveIngestStop"
echo "      Status command: datawaveIngestStatus"
echo "        Test command: datawaveIngestWikipedia <raw-enwiki-file>"
echo "        Test command: datawaveIngestCsv <raw-csv-file>"
echo "        Test command: datawaveIngestJson <raw-json-file>"
echo "       Build command: datawaveBuild"
echo "    Redeploy command: datawaveBuildDeploy"
echo
info "See \$DW_CLOUD_HOME/bin/services/datawave/bootstrap-ingest.sh to view/edit commands as needed"

# Ingest raw data examples, if appropriate...

[ "${DW_REDEPLOY_IN_PROGRESS}" != true ] && [ "${DW_DATAWAVE_INGEST_TEST_SKIP}" == false ] && datawaveIngestExamples
