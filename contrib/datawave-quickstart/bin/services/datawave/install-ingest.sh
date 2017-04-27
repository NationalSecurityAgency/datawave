# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"
source "${SERVICES_DIR}/hadoop/bootstrap.sh"
source "${SERVICES_DIR}/accumulo/bootstrap.sh"

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
if [ -n "${DW_DATAWAVE_INGEST_HDFS_INPUTS}" ]; then
   OLD_IFS=$IFS
   IFS=','
   read -r -a INGEST_HDFS_INPUT_ARRAY <<< "$DW_DATAWAVE_INGEST_HDFS_INPUTS"
   IFS=$OLD_IFS
   # Create the HDFS directories
   for dir in "${INGEST_HDFS_INPUT_ARRAY[@]}"
   do
      "${HADOOP_HOME}"/bin/hdfs dfs -mkdir -p "${dir}" || fatal "Failed to create HDFS directory "${dir}""
   done
fi

#----------------------------------------------------------
# Configure/update Accumulo classpath, set auths, etc
#----------------------------------------------------------

if accumuloIsRunning; then
  info "Stopping Accumulo to update classpath, etc..."
  accumuloStop || fatal "Failed to stop an already running Accumulo"
fi

# Update Accumulo classpath for DataWave jars in HDFS/VFS directories (or lib/ext)

if [ -n "${DW_ACCUMULO_VFS_DATAWAVE_DIR}" ]; then
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
   [ ! -d ${ACCUMULO_HOME}/lib/ext ] && fatal "Unable to update Accumulo classpath. ${ACCUMULO_HOME}/lib/ext does not exist!"
   info "Removing any existing jars from ${ACCUMULO_HOME}/lib/ext"
   rm -f ${ACCUMULO_HOME}/lib/ext/*.jar
   info "Copying DataWave jars into ${ACCUMULO_HOME}/lib/ext"
   if [ -d ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib ]; then
      cp ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/*.jar ${ACCUMULO_HOME}/lib/ext
   fi
   if [ -d ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/ext ]; then
      cp ${DW_DATAWAVE_INGEST_HOME}/accumulo-warehouse/lib/ext/*.jar ${ACCUMULO_HOME}/lib/ext
   fi
fi

# Make sure Accumulo service is started.
if ! accumuloIsRunning ; then
   info "Starting Accumulo, so that we can initialize DataWave Ingest"
   accumuloStart
fi

# Perform additional Accumulo configs dynamically as needed.
ACCUMULO_TMP_SCRIPT="$(mktemp -t `basename $0`-accumulo-shell.XXXXX)"
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

echo
info "DataWave Ingest initialized and ready to start..."
echo
echo "       Start command: datawaveIngestStart"
echo "        Stop command: datawaveIngestStop"
echo "      Status command: datawaveIngestStatus"
echo "        Test command: datawaveIngestWikipedia <raw-enwiki-file>"
echo "    Redeploy command: datawaveBuildDeploy"
echo
info "See \$DW_CLOUD_HOME/bin/services/datawave/bootstrap-ingest.sh to view/edit commands as needed"

OK_TO_INGEST_WIKIPEDIA=true
if [ "${DW_REDEPLOY_IN_PROGRESS}" == true ] ; then
    if ! askYesNo "Do you want to ingest '${DW_DATAWAVE_INGEST_TEST_FILE}' ?" ; then
        OK_TO_INGEST_WIKIPEDIA=false
    fi
fi

if [ "${OK_TO_INGEST_WIKIPEDIA}" == true ] ; then
    # First run a job to create all necessary tables
    datawaveIngestCreateTables
    # Lastly, ingest wikipedia rawfile
    datawaveIngestWikipedia "${DW_DATAWAVE_INGEST_TEST_FILE}"
fi
