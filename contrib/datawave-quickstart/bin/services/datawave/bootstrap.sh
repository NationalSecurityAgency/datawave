# Sourced by env.sh

# This bootstrap sources both bootstrap-ingest.sh and bootstrap-web.sh, so that
# higher level scripts can harness both of those services via the "datawave"
# service name, and so that there's a single place to define variables and code
# shared by both components

# Current script dir

DW_DATAWAVE_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Source/repository root

DW_DATAWAVE_SOURCE_DIR="$( cd "${DW_DATAWAVE_SERVICE_DIR}/../../../../.." && pwd )"

# Comma-delimited list of Accumulo authorizations to grant DataWave's Accumulo user. These will be automatically
# assigned to that user with a dynamically generated accumulo-shell script during the DataWave install. Override the
# default list as needed for whatever auths your test data requires. Defaults to authorizations known to exist on our
# canned example data. Should be exhaustive for any and all known/required auths. Otherwise, you will not be able to
# view the data in Accumulo Shell

DW_DATAWAVE_ACCUMULO_AUTHS="${DW_DATAWAVE_ACCUMULO_AUTHS:-PUBLIC,PRIVATE,FOO,BAR,DEF,A,B,C,D,E,F,G,H,I,DW_USER,DW_SERV,DW_ADMIN,JBOSS_ADMIN}"

# Import DataWave Web test user configuration

source "${DW_DATAWAVE_SERVICE_DIR}/bootstrap-user.sh"

# Selected Maven profile for the DataWave build

DW_DATAWAVE_BUILD_PROFILE=${DW_DATAWAVE_BUILD_PROFILE:-dev}

# Maven command
DW_DATAWAVE_BUILD_COMMAND="${DW_DATAWAVE_BUILD_COMMAND:-mvn -P${DW_DATAWAVE_BUILD_PROFILE} -Ddeploy -Dtar -Ddist -DskipServices -DskipTests clean package --builder smart -T1.0C}"

# Home of any temp data and *.properties file overrides for this instance of DataWave

DW_DATAWAVE_DATA_DIR="${DW_CLOUD_DATA}/datawave"

# Temp dir for persisting our dynamically-generated ${DW_DATAWAVE_BUILD_PROFILE}.properties file

DW_DATAWAVE_BUILD_PROPERTIES_DIR="${DW_DATAWAVE_DATA_DIR}/build-properties"

DW_DATAWAVE_BUILD_STATUS_LOG="${DW_DATAWAVE_BUILD_PROPERTIES_DIR}/build-progress.tmp"

DW_DATAWAVE_INGEST_TARBALL="*/datawave-${DW_DATAWAVE_BUILD_PROFILE}-*-dist.tar.gz"

DW_DATAWAVE_WEB_TARBALL="*/datawave-ws-deploy-application-*-${DW_DATAWAVE_BUILD_PROFILE}.tar.gz"

DW_DATAWAVE_KEYSTORE="${DW_DATAWAVE_KEYSTORE:-${DW_DATAWAVE_SOURCE_DIR}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testServer.p12}"

DW_DATAWAVE_KEYSTORE_PASSWORD=${DW_DATAWAVE_KEYSTORE_PASSWORD:-ChangeIt}

DW_DATAWAVE_KEYSTORE_TYPE="${DW_DATAWAVE_KEYSTORE_TYPE:-PKCS12}"

DW_DATAWAVE_TRUSTSTORE="${DW_DATAWAVE_TRUSTSTORE:-${DW_DATAWAVE_SOURCE_DIR}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/ca.jks}"

DW_DATAWAVE_TRUSTSTORE_PASSWORD=${DW_DATAWAVE_TRUSTSTORE_PASSWORD:-ChangeIt}

DW_DATAWAVE_TRUSTSTORE_TYPE="${DW_DATAWAVE_TRUSTSTORE_TYPE:-JKS}"

# Accumulo shell script for initializing whatever we may need in Accumulo for DataWave

function createAccumuloShellInitScript() {
   # Allow user to inject their own script into the env...
   [ -n "${DW_ACCUMULO_SHELL_INIT_SCRIPT}" ] && return 0

   # Create script and add 'datawave' VFS context, if enabled...

   DW_ACCUMULO_SHELL_INIT_SCRIPT="
   createnamespace datawave
   createtable datawave.queryMetrics_m
   createtable datawave.queryMetrics_s
   setauths -s ${DW_DATAWAVE_ACCUMULO_AUTHS}"

   if [ "${DW_ACCUMULO_VFS_DATAWAVE_ENABLED}" != false ] ; then
      DW_ACCUMULO_SHELL_INIT_SCRIPT="${DW_ACCUMULO_SHELL_INIT_SCRIPT}
   config -s table.class.loader.context=datawave"
   else
      DW_ACCUMULO_SHELL_INIT_SCRIPT="${DW_ACCUMULO_SHELL_INIT_SCRIPT}
   config -s table.class.loader.context=extlib"
   fi

   DW_ACCUMULO_SHELL_INIT_SCRIPT="${DW_ACCUMULO_SHELL_INIT_SCRIPT}
   quit
   "
}

function createBuildPropertiesDirectory() {
   if [ ! -d ${DW_DATAWAVE_BUILD_PROPERTIES_DIR} ] ; then
      if ! mkdir -p ${DW_DATAWAVE_BUILD_PROPERTIES_DIR} ; then
         error "Failed to create directory ${DW_DATAWAVE_BUILD_PROPERTIES_DIR}"
         return 1
      fi
   fi
   return 0
}

function setBuildPropertyOverrides() {

   # DataWave's build configs (*.properties) can be loaded from a variety of locations based on the 'read-properties'
   # Maven plugin configuration. Typically, the source-root/properties/*.properties files are loaded first to provide
   # default values, starting with 'default.properties', followed by '{selected-profile}.properties'. Finally,
   # ~/.m2/datawave/properties/{selected-profile}.properties is loaded, if it exists, allowing you to override
   # defaults as needed

   # With that in mind, the goal of this function is to generate a new '${DW_DATAWAVE_BUILD_PROFILE}.properties' file under
   # DW_DATAWAVE_BUILD_PROPERTIES_DIR and *symlinked* as ~/.m2/datawave/properties/${DW_DATAWAVE_BUILD_PROFILE}.properties,
   # to inject all the overrides that we need for successful deployment to source-root/contrib/datawave-quickstart/

   # If a file having the name '${DW_DATAWAVE_BUILD_PROFILE}.properties' already exists under ~/.m2/datawave/properties,
   # then it will be renamed automatically with a ".saved-by-quickstart-$(date)" suffix, and the symlink for the new
   # file will be created as required

   local BUILD_PROPERTIES_BASENAME=${DW_DATAWAVE_BUILD_PROFILE}.properties
   local BUILD_PROPERTIES_FILE=${DW_DATAWAVE_BUILD_PROPERTIES_DIR}/${BUILD_PROPERTIES_BASENAME}
   local BUILD_PROPERTIES_SYMLINK_DIR=${HOME}/.m2/datawave/properties
   local BUILD_PROPERTIES_SYMLINK=${BUILD_PROPERTIES_SYMLINK_DIR}/${BUILD_PROPERTIES_BASENAME}

   ! createBuildPropertiesDirectory && error "Failed to override properties!" && return 1

   # Create symlink directory if it doesn't exist
   [ ! -d ${BUILD_PROPERTIES_SYMLINK_DIR} ] \
       && ! mkdir -p ${BUILD_PROPERTIES_SYMLINK_DIR} \
       && error "Failed to create symlink directory ${BUILD_PROPERTIES_SYMLINK_DIR}" \
       && return 1

   # Copy existing source-root/properties/${DW_DATAWAVE_BUILD_PROFILE}.properties to our new $BUILD_PROPERTIES_FILE
   ! cp "${DW_DATAWAVE_SOURCE_DIR}/properties/${DW_DATAWAVE_BUILD_PROFILE}.properties" ${BUILD_PROPERTIES_FILE} \
       && error "Aborting property overrides! Failed to copy ${DW_DATAWAVE_BUILD_PROFILE}.properties" \
       && return 1

   # Apply overrides as needed by simply appending them to the end of the file...

   echo "#" >> ${BUILD_PROPERTIES_FILE}
   echo "######## Begin overrides for datawave-quickstart ########" >> ${BUILD_PROPERTIES_FILE}
   echo "#" >> ${BUILD_PROPERTIES_FILE}

   echo "WAREHOUSE_ACCUMULO_HOME=${ACCUMULO_HOME}" >> ${BUILD_PROPERTIES_FILE}
   echo "WAREHOUSE_INSTANCE_NAME=${DW_ACCUMULO_INSTANCE_NAME}" >> ${BUILD_PROPERTIES_FILE}
   echo "WAREHOUSE_JOBTRACKER_NODE=${DW_HADOOP_RESOURCE_MANAGER_ADDRESS_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "INGEST_ACCUMULO_HOME=${ACCUMULO_HOME}" >> ${BUILD_PROPERTIES_FILE}
   echo "INGEST_INSTANCE_NAME=${DW_ACCUMULO_INSTANCE_NAME}" >> ${BUILD_PROPERTIES_FILE}
   echo "INGEST_JOBTRACKER_NODE=${DW_HADOOP_RESOURCE_MANAGER_ADDRESS_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "BULK_INGEST_DATA_TYPES=${DW_DATAWAVE_INGEST_BULK_DATA_TYPES}" >> ${BUILD_PROPERTIES_FILE}
   echo "LIVE_INGEST_DATA_TYPES=${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES}" >> ${BUILD_PROPERTIES_FILE}
   echo "PASSWORD=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "ZOOKEEPER_HOME=${ZOOKEEPER_HOME}" >> ${BUILD_PROPERTIES_FILE}
   echo "HADOOP_HOME=${HADOOP_HOME}" >> ${BUILD_PROPERTIES_FILE}
   echo "MAPRED_HOME=${HADOOP_HOME}" >> ${BUILD_PROPERTIES_FILE}
   echo "WAREHOUSE_HADOOP_CONF=${HADOOP_CONF_DIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "INGEST_HADOOP_CONF=${HADOOP_CONF_DIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "HDFS_BASE_DIR=${DW_DATAWAVE_INGEST_HDFS_BASEDIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "MAPRED_INGEST_OPTS=${DW_DATAWAVE_MAPRED_INGEST_OPTS}" >> ${BUILD_PROPERTIES_FILE}
   echo "LOG_DIR=${DW_DATAWAVE_INGEST_LOG_DIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "FLAG_DIR=${DW_DATAWAVE_INGEST_FLAGFILE_DIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "FLAG_MAKER_CONFIG=${DW_DATAWAVE_INGEST_FLAGMAKER_CONFIGS}" >> ${BUILD_PROPERTIES_FILE}
   echo "BIN_DIR_FOR_FLAGS=${DW_DATAWAVE_INGEST_HOME}/bin" >> ${BUILD_PROPERTIES_FILE}
   echo "KEYSTORE=${DW_DATAWAVE_KEYSTORE}" >> ${BUILD_PROPERTIES_FILE}
   echo "KEYSTORE_TYPE=${DW_DATAWAVE_KEYSTORE_TYPE}" >> ${BUILD_PROPERTIES_FILE}
   echo "KEYSTORE_PASSWORD=${DW_DATAWAVE_KEYSTORE_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "TRUSTSTORE=${DW_DATAWAVE_TRUSTSTORE}" >> ${BUILD_PROPERTIES_FILE}
   echo "TRUSTSTORE_PASSWORD=${DW_DATAWAVE_TRUSTSTORE_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "TRUSTSTORE_TYPE=${DW_DATAWAVE_TRUSTSTORE_TYPE}" >> ${BUILD_PROPERTIES_FILE}
   echo "FLAG_METRICS_DIR=${DW_DATAWAVE_INGEST_FLAGMETRICS_DIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "accumulo.instance.name=${DW_ACCUMULO_INSTANCE_NAME}" >> ${BUILD_PROPERTIES_FILE}
   echo "accumulo.user.password=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}

#   # uncomment to enable environment passwords in the quickstart, and comment out above line
#   echo "accumulo.user.password=env:DW_ACCUMULO_PASSWORD" >> ${BUILD_PROPERTIES_FILE}

   echo "cached.results.hdfs.uri=${DW_HADOOP_DFS_URI_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "type.metadata.hdfs.uri=${DW_HADOOP_DFS_URI_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "mapReduce.hdfs.uri=${DW_HADOOP_DFS_URI_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "bulkResults.hdfs.uri=${DW_HADOOP_DFS_URI_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "jboss.log.hdfs.uri=${DW_HADOOP_DFS_URI_CLIENT}" >> ${BUILD_PROPERTIES_FILE}

   echo "lock.file.dir=${DW_DATAWAVE_INGEST_LOCKFILE_DIR}" >> ${BUILD_PROPERTIES_FILE}
   echo "server.keystore.password=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "mysql.user.password=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "jboss.jmx.password=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "jboss.managed.executor.service.default.max.threads=${DW_WILDFLY_EE_DEFAULT_MAX_THREADS:-48}" >> ${BUILD_PROPERTIES_FILE}
   echo "hornetq.cluster.password=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "hornetq.system.password=${DW_ACCUMULO_PASSWORD}" >> ${BUILD_PROPERTIES_FILE}
   echo "mapReduce.job.tracker=${DW_HADOOP_RESOURCE_MANAGER_ADDRESS_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "bulkResults.job.tracker=${DW_HADOOP_RESOURCE_MANAGER_ADDRESS_CLIENT}" >> ${BUILD_PROPERTIES_FILE}
   echo "EVENT_DISCARD_INTERVAL=0" >> ${BUILD_PROPERTIES_FILE}
   echo "ingest.data.types=${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES},${DW_DATAWAVE_INGEST_BULK_DATA_TYPES}" >> ${BUILD_PROPERTIES_FILE}
   echo "JOB_CACHE_REPLICATION=1" >> ${BUILD_PROPERTIES_FILE}
   echo "EDGE_DEFINITION_FILE=${DW_DATAWAVE_INGEST_EDGE_DEFINITIONS}" >> ${BUILD_PROPERTIES_FILE}
   echo "DATAWAVE_INGEST_HOME=${DW_DATAWAVE_INGEST_HOME}" >> ${BUILD_PROPERTIES_FILE}
   echo "PASSWORD_INGEST_ENV=${DW_DATAWAVE_INGEST_PASSWD_FILE}" >> ${BUILD_PROPERTIES_FILE}
   echo "hdfs.site.config.urls=file://${HADOOP_CONF_DIR}/core-site.xml,file://${HADOOP_CONF_DIR}/hdfs-site.xml" >> ${BUILD_PROPERTIES_FILE}
   echo "table.shard.numShardsPerDay=${DW_DATAWAVE_INGEST_NUM_SHARDS}" >> ${BUILD_PROPERTIES_FILE}

   generateTestDatawaveUserServiceConfig

   # Apply DW_JAVA_HOME_OVERRIDE, if needed...
   # We can override the JAVA_HOME location for the DataWave deployment, if necessary. E.g., if we're deploying
   # to a Docker container or other, where our current JAVA_HOME isn't applicable

   if [ -n "${DW_JAVA_HOME_OVERRIDE}" ] ; then
      echo "JAVA_HOME=${DW_JAVA_HOME_OVERRIDE}" >> ${BUILD_PROPERTIES_FILE}
   else
      echo "JAVA_HOME=${JAVA_HOME}" >> ${BUILD_PROPERTIES_FILE}
   fi

   # Apply DW_ROOT_DIRECTORY_OVERRIDE, if needed...
   # We can override any instances of DW_DATAWAVE_SOURCE_DIR within the build config in order to relocate
   # the deployment, if necessary. E.g., used when building the datawave-quickstart Docker image to reorient
   # the deployment under /opt/datawave/ within the container

   if [ -n "${DW_ROOT_DIRECTORY_OVERRIDE}" ] ; then
      sed -i "s~${DW_DATAWAVE_SOURCE_DIR}~${DW_ROOT_DIRECTORY_OVERRIDE}~g" ${BUILD_PROPERTIES_FILE}
   fi

   # Create the symlink under ~/.m2/datawave/properties

   setBuildPropertiesSymlink || return 1
}

function setBuildPropertiesSymlink() {
   # Replace any existing ~/.m2/datawave/properties/${BUILD_PROPERTIES_BASENAME} file/symlink with
   # a symlink to our new ${BUILD_PROPERTIES_FILE}

   if [[ -f ${BUILD_PROPERTIES_SYMLINK} || -L ${BUILD_PROPERTIES_SYMLINK} ]] ; then
       if [ -L ${BUILD_PROPERTIES_SYMLINK} ] ; then
           info "Unlinking existing symbolic link: ${BUILD_PROPERTIES_SYMLINK}"
           if ! unlink "${BUILD_PROPERTIES_SYMLINK}" ; then
               warn "Failed to unlink $( readlink ${BUILD_PROPERTIES_SYMLINK} ) from ${BUILD_PROPERTIES_SYMLINK_DIR}"
           fi
       else
           local backupFile="${BUILD_PROPERTIES_SYMLINK}.saved-by-quickstart.$(date +%Y-%m-%d-%H%M%S)"
           info "Backing up your existing ~/.m2/**/${BUILD_PROPERTIES_BASENAME} file to ~/.m2/**/$( basename ${backupFile} )"
           if ! mv "${BUILD_PROPERTIES_SYMLINK}" "${backupFile}" ; then
               error "Failed to backup ${BUILD_PROPERTIES_SYMLINK}. Aborting properties file override. Please fix me!!"
               return 1
           fi
       fi
   fi

   if ln -s "${BUILD_PROPERTIES_FILE}" "${BUILD_PROPERTIES_SYMLINK}" ; then
       info "Override for ${BUILD_PROPERTIES_BASENAME} successful"
   else
       error "Override for ${BUILD_PROPERTIES_BASENAME} failed"
       return 1
   fi
}

function datawaveBuildSucceeded() {
   local success=$( tail -n 7 "$DW_DATAWAVE_BUILD_STATUS_LOG" | grep "BUILD SUCCESS" )
   if [ -z "${success}" ] ; then
       return 1
   fi
   return 0
}

function buildDataWave() {

   if ! mavenIsInstalled ; then
      ! mavenInstall && error "Maven install failed. Please correct" && return 1
   fi

   [[ "$1" == "--verbose" ]] && local verbose=true

   ! setBuildPropertyOverrides && error "Aborting DataWave build" && return 1

   [ -f "${DW_DATAWAVE_BUILD_STATUS_LOG}" ] && rm -f "$DW_DATAWAVE_BUILD_STATUS_LOG"

   info "DataWave build in progress: '${DW_DATAWAVE_BUILD_COMMAND}'"
   info "Build status log: $DW_DATAWAVE_BUILD_STATUS_LOG"
   if [ "${verbose}" == true ] ; then
       ( cd "${DW_DATAWAVE_SOURCE_DIR}" && eval "${DW_DATAWAVE_BUILD_COMMAND}" 2>&1 | tee ${DW_DATAWAVE_BUILD_STATUS_LOG} )
   else
       ( cd "${DW_DATAWAVE_SOURCE_DIR}" && eval "${DW_DATAWAVE_BUILD_COMMAND}" &> ${DW_DATAWAVE_BUILD_STATUS_LOG} )
   fi

   if ! datawaveBuildSucceeded ; then
       error "The build has FAILED! See $DW_DATAWAVE_BUILD_STATUS_LOG for details"
       return 1
   fi

   info "DataWave build successful"
   return 0
}

function getDataWaveTarball() {
   # Looks for a DataWave tarball matching the specified pattern and, if found, sets the global 'tarball'
   # variable to its basename for the caller as expected.

   # If no tarball is found matching the specified pattern, then the DataWave build is kicked off

   local tarballPattern="${1}"
   tarball=""

   # Check if the tarball already exists in the plugin directory.
   local tarballPath="$( find "${DW_DATAWAVE_SERVICE_DIR}" -path "${tarballPattern}" -type f )"
   if [ -f "${tarballPath}" ]; then
      tarball="$( basename "${tarballPath}" )"
      return 0;
   fi

   ! buildDataWave --verbose && error "Please correct this issue before continuing" && return 1

   # Build succeeded. Set global 'tarball' variable for the specified pattern and copy all tarballs into place

   tarballPath="$( find "${DW_DATAWAVE_SOURCE_DIR}" -path "${tarballPattern}" -type f | tail -1 )"
   [ -z "${tarballPath}" ] && error "Failed to find '${tarballPattern}' tar file after build" && return 1

   tarball="$( basename "${tarballPath}" )"

   # Current caller (ie, either bootstrap-web.sh or bootstrap-ingest.sh) only cares about current $tarball,
   # but go ahead and copy both tarballs into datawave service dir to satisfy next caller as well

   ! copyDataWaveTarball "${DW_DATAWAVE_INGEST_TARBALL}" && error "Failed to copy DataWave Ingest tarball" && return 1
   ! copyDataWaveTarball "${DW_DATAWAVE_WEB_TARBALL}" && error "Failed to copy DataWave Web tarball" && return 1

   return 0
}

function copyDataWaveTarball() {
   local pattern="${1}"
   local dwTarball="$( find "${DW_DATAWAVE_SOURCE_DIR}" -path "${pattern}" -type f | tail -1 )";
   if [ -n "${dwTarball}" ] ; then
       ! cp "${dwTarball}" "${DW_DATAWAVE_SERVICE_DIR}" && error "Failed to copy '${dwTarball}'" && return 1
   else
       error "No tar file found matching '${pattern}'"
       return 1
   fi
   return 0
}

# Bootstrap DW ingest and webservice components as needed

source "${DW_DATAWAVE_SERVICE_DIR}/bootstrap-ingest.sh"
source "${DW_DATAWAVE_SERVICE_DIR}/bootstrap-web.sh"

function datawaveIsRunning() {
    datawaveIngestIsRunning && return 0
    datawaveWebIsRunning && return 0
    return 1
}

function datawaveStart() {
    datawaveIngestStart
    datawaveWebStart
}

function datawaveStop() {
    datawaveIngestStop
    datawaveWebStop
}

function datawaveStatus() {
    datawaveIngestStatus
    datawaveWebStatus
}

function datawaveIsInstalled() {
    datawaveIngestIsInstalled && return 0
    datawaveWebIsInstalled && return 0
    return 1
}

function datawaveUninstall() {
   datawaveIngestUninstall
   datawaveWebUninstall

   [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_DATAWAVE_SERVICE_DIR}"/datawave*.tar.gz
}

function datawaveInstall() {
   datawaveIngestInstall
   datawaveWebInstall
}

function datawavePrintenv() {
   echo
   echo "DataWave Environment"
   echo
   ( set -o posix ; set ) | grep -E "DATAWAVE_|WILDFLY|JBOSS"
   echo
}

function datawavePidList() {
   datawaveIngestIsRunning
   datawaveWebIsRunning
   if [[ -n "${DW_DATAWAVE_WEB_PID_LIST}" || -n "${DW_DATAWAVE_INGEST_PID_LIST}" ]] ; then
      echo "${DW_DATAWAVE_WEB_PID_LIST} ${DW_DATAWAVE_INGEST_PID_LIST}"
   fi
}

function datawaveBuildDeploy() {
   datawaveIsRunning && info "Stopping all DataWave services" && datawaveStop
   datawaveIsInstalled && info "Uninstalling DataWave" && datawaveUninstall --remove-binaries

   resetQuickstartEnvironment
   export DW_REDEPLOY_IN_PROGRESS=true
   datawaveInstall
   export DW_REDEPLOY_IN_PROGRESS=false
}

function datawaveBuild() {
   info "Building DataWave"
   rm -f "${DW_DATAWAVE_SERVICE_DIR}"/datawave*.tar.gz
   resetQuickstartEnvironment
}

function datawaveDisplayBinaryInfo() {
   datawaveIngestDisplayBinaryInfo
   datawaveWebDisplayBinaryInfo
}
