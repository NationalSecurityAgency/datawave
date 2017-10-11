# Sourced by env.sh

DW_DATAWAVE_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DW_DATAWAVE_SOURCE_DIR="$( cd "${DW_DATAWAVE_SERVICE_DIR}/../../../../.." && pwd )"

# Home of temp data and build property overrides for this instance of DataWave
DW_DATAWAVE_DATA_DIR="${DW_CLOUD_DATA}/datawave"

DW_DATAWAVE_BUILD_COMMAND="mvn -Pdev -Ddeploy -Dtar -Ddist -DskipTests -DskipITs clean install"
DW_DATAWAVE_BUILD_PROPERTIES_DIR="${DW_DATAWAVE_DATA_DIR}/build-properties"
DW_DATAWAVE_BUILD_STATUS_LOG="${DW_DATAWAVE_BUILD_PROPERTIES_DIR}/build-progress.tmp"

DW_DATAWAVE_INGEST_TARBALL="*/datawave-dev-*-dist.tar.gz"
DW_DATAWAVE_WEB_TARBALL="*/datawave-ws-deploy-application-*-dev.tar.gz"

DW_DATAWAVE_KEYSTORE="${DW_DATAWAVE_SOURCE_DIR}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testServer.p12"
DW_DATAWAVE_KEYSTORE_TYPE="PKCS12"
DW_DATAWAVE_TRUSTSTORE="${DW_DATAWAVE_SOURCE_DIR}/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/ca.jks"
DW_DATAWAVE_TRUSTSTORE_TYPE="JKS"

function createBuildPropertiesDirectory() {
   [ ! -d ${DW_DATAWAVE_BUILD_PROPERTIES_DIR} ] && ! mkdir -p ${DW_DATAWAVE_BUILD_PROPERTIES_DIR} && error "Failed to create directory ${DW_DATAWAVE_BUILD_PROPERTIES_DIR}"
}

function setBuildPropertyOverrides() {
   # The purpose of this function is to inject certain property-value overrides
   # into the Maven build, so that the DW deployment's required runtime directories
   # are configured in a portable manner, i.e., relative to the location of your
   # datawave-quickstart root directory.

   # Here we copy the CM'ed dev.properties file to DW_DATAWAVE_DATA_DIR/build-properties,
   # inject overrides, and then create a '~/.m2/datawave/properties/dev.properties' symlink to it

   local BUILD_PROPERTIES_BASENAME=dev.properties
   local BUILD_PROPERTIES_FILE=${DW_DATAWAVE_BUILD_PROPERTIES_DIR}/${BUILD_PROPERTIES_BASENAME}
   local BUILD_PROPERTIES_SYMLINK_DIR=${HOME}/.m2/datawave/properties
   local BUILD_PROPERTIES_SYMLINK=${BUILD_PROPERTIES_SYMLINK_DIR}/${BUILD_PROPERTIES_BASENAME}

   createBuildPropertiesDirectory

   # Create symlink directory if it doesn't exist
   [ ! -d ${BUILD_PROPERTIES_SYMLINK_DIR} ] && ! mkdir -p ${BUILD_PROPERTIES_SYMLINK_DIR} && error "Failed to create symlink directory ${BUILD_PROPERTIES_SYMLINK_DIR}"

   if [ ! -e "${BUILD_PROPERTIES_FILE}" ]; then
      # Copy dev.properties from the source code directory to the ./build-properties directory
      ! cp "${DW_DATAWAVE_SOURCE_DIR}/properties/${BUILD_PROPERTIES_BASENAME}" ${BUILD_PROPERTIES_FILE} && error "Failed to copy ${BUILD_PROPERTIES_FILE}"

      # Override dev.properties with custom values
      sed -i "s~\(WAREHOUSE_ACCUMULO_HOME=\).*$~\1${ACCUMULO_HOME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(WAREHOUSE_INSTANCE_NAME=\).*$~\1${DW_ACCUMULO_INSTANCE_NAME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(WAREHOUSE_JOBTRACKER_NODE=\).*$~\1${DW_HADOOP_RESOURCE_MANAGER_ADDRESS}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(INGEST_ACCUMULO_HOME=\).*$~\1${ACCUMULO_HOME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(INGEST_INSTANCE_NAME=\).*$~\1${DW_ACCUMULO_INSTANCE_NAME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(INGEST_JOBTRACKER_NODE=\).*$~\1${DW_HADOOP_RESOURCE_MANAGER_ADDRESS}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(BULK_INGEST_DATA_TYPES=\).*$~\1${DW_DATAWAVE_INGEST_BULK_DATA_TYPES}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(LIVE_INGEST_DATA_TYPES=\).*$~\1${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(PASSWORD=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(ZOOKEEPER_HOME=\).*$~\1${ZOOKEEPER_HOME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(HADOOP_HOME=\).*$~\1${HADOOP_HOME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(MAPRED_HOME=\).*$~\1${HADOOP_HOME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(WAREHOUSE_HADOOP_CONF=\).*$~\1${HADOOP_CONF_DIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(INGEST_HADOOP_CONF=\).*$~\1${HADOOP_CONF_DIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(HDFS_BASE_DIR=\).*$~\1${DW_DATAWAVE_INGEST_HDFS_BASEDIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(MAPRED_INGEST_OPTS=\).*$~\1${DW_DATAWAVE_MAPRED_INGEST_OPTS}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(LOG_DIR=\).*$~\1${DW_DATAWAVE_INGEST_LOG_DIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(FLAG_DIR=\).*$~\1${DW_DATAWAVE_INGEST_FLAGFILE_DIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(FLAG_MAKER_CONFIG=\).*$~\1${DW_DATAWAVE_INGEST_FLAGMAKER_CONFIGS}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(BIN_DIR_FOR_FLAGS=\).*$~\1${DW_DATAWAVE_INGEST_HOME}\/bin~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(KEYSTORE=\).*$~\1${DW_DATAWAVE_KEYSTORE}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(KEYSTORE_TYPE=\).*$~\1${DW_DATAWAVE_KEYSTORE_TYPE}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(KEYSTORE_PASSWORD=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(TRUSTSTORE=\).*$~\1${DW_DATAWAVE_TRUSTSTORE}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(FLAG_METRICS_DIR=\).*$~\1${DW_DATAWAVE_INGEST_FLAGMETRICS_DIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(TRUSTSTORE_TYPE=\).*$~\1${DW_DATAWAVE_TRUSTSTORE_TYPE}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(accumulo\.instance\.name=\).*$~\1${DW_ACCUMULO_INSTANCE_NAME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(accumulo\.user\.password=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(cached\.results\.hdfs\.uri=\).*$~\1${DW_HADOOP_DFS_URI}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(lock\.file\.dir=\).*$~\1${DW_DATAWAVE_INGEST_LOCKFILE_DIR}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(JAVA_HOME=\).*$~\1${JAVA_HOME}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(server\.keystore\.password=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(mysql\.user\.password=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(jboss\.jmx\.password=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(hornetq\.cluster\.password=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(hornetq\.system\.password=\).*$~\1${DW_ACCUMULO_PASSWORD}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(mapReduce\.job\.tracker=\).*$~\1${DW_HADOOP_RESOURCE_MANAGER_ADDRESS}~g" ${BUILD_PROPERTIES_FILE}
      sed -i "s~\(bulkResults\.job\.tracker=\).*$~\1${DW_HADOOP_RESOURCE_MANAGER_ADDRESS}~g" ${BUILD_PROPERTIES_FILE}

      # Override the following according to wikipedia.properties

      sed -i "s~\(EVENT_DISCARD_INTERVAL=\).*$~\10~g" ${BUILD_PROPERTIES_FILE}
      echo "ingest.data.types=${DW_DATAWAVE_INGEST_LIVE_DATA_TYPES}" >> ${BUILD_PROPERTIES_FILE}
      echo "JOB_CACHE_REPLICATION=1" >> ${BUILD_PROPERTIES_FILE}
      echo "EDGE_DEFINITION_FILE=config/WikiEdgeSpringConfig.xml" >> ${BUILD_PROPERTIES_FILE}

      # Append the following to the dev.properties file
      echo "DATAWAVE_INGEST_HOME=${DW_DATAWAVE_INGEST_HOME}" >> ${BUILD_PROPERTIES_FILE}
      echo "PASSWORD_INGEST_ENV=${DW_DATAWAVE_INGEST_PASSWD_FILE}" >> ${BUILD_PROPERTIES_FILE}
      echo "hdfs.site.config.urls=file://${HADOOP_CONF_DIR}/core-site.xml,file://${HADOOP_CONF_DIR}/hdfs-site.xml" >> ${BUILD_PROPERTIES_FILE}
   fi

   # We can override any instances of DW_DATAWAVE_SOURCE_DIR within the build config in order to relocate
   # the deployment, if necessary. For example, this is used when building the datawave-quickstart Docker
   # image to reorient the deployment under /opt/datawave/ within the container
   if [ -n "${DW_ROOT_DIRECTORY_OVERRIDE}" ] ; then
      sed -i "s~${DW_DATAWAVE_SOURCE_DIR}~${DW_ROOT_DIRECTORY_OVERRIDE}~g" ${BUILD_PROPERTIES_FILE}
   fi

   # As with DW_ROOT_DIRECTORY_OVERRIDE, override JAVA_HOME for the deployment if necessary
   if [ -n "${DW_JAVA_HOME_OVERRIDE}" ] ; then
      sed -i "s~\(JAVA_HOME=\).*$~\1${DW_JAVA_HOME_OVERRIDE}~g" ${BUILD_PROPERTIES_FILE}
   fi

   # Replace any existing ~/.m2/*/dev.properties file/symlink with our own

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
   fi
}

function buildRequiredPlugins() {
    ( cd "${DW_DATAWAVE_SOURCE_DIR}/contrib/assert-properties" && mvn clean install )
    ( cd "${DW_DATAWAVE_SOURCE_DIR}/contrib/read-properties" && mvn clean install )
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

   setBuildPropertyOverrides

   [ -f "${DW_DATAWAVE_BUILD_STATUS_LOG}" ] && rm -f "$DW_DATAWAVE_BUILD_STATUS_LOG"

   buildRequiredPlugins

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
   local tarballPattern="$1"
   tarball=""

   # Check if the tarball already exists in the plugin directory.
   local tarballPath="$( find "${DW_DATAWAVE_SERVICE_DIR}" -path "${tarballPattern}" -type f )"
   if [ -f "${tarballPath}" ]; then
      tarball="$( basename "${tarballPath}" )"
      return 0;
   fi

   # Ensure that java is installed and JAVA_HOME set before we try to clean/build
   source "${DW_CLOUD_HOME}/bin/services/java/bootstrap.sh"
   ! javaIsInstalled && javaInstall
   javaIsInstalled || error "Java bootstrap failed. DataWave build may not succeed"

   ! buildDataWave --verbose && error "Please correct this issue before continuing" && return 1

   # Try to find it after building datawave
   tarballPath="$( find "${DW_DATAWAVE_SOURCE_DIR}" -path "${tarballPattern}" -type f | tail -1 )"
   [ -z "${tarballPath}" ] && error "Failed to find ingest tar file after build" && return 1

   tarball="$( basename "${tarballPath}" )"
   # Copy tar file to the tarball directory
   ( cp "${tarballPath}" "${DW_DATAWAVE_SERVICE_DIR}" ) || error "Failed to copy tar file"

   # Go ahead and copy the *other* tarball into place as well
   copyOtherTarball "${tarballPattern}"
}

function copyOtherTarball() {
   local tarballPattern="$1"
   # Specified tarballPattern will have already been copied into place, so just get the other one
   if [ "${tarballPattern}" == "${DW_DATAWAVE_INGEST_TARBALL}" ] ; then
      local webserviceTarball="$( find "${DW_DATAWAVE_SOURCE_DIR}" -path "${DW_DATAWAVE_WEB_TARBALL}" -type f | tail -1 )";
      ( cp "${webserviceTarball}" "${DW_DATAWAVE_SERVICE_DIR}" ) || error "Failed to copy web service tar file"
   else
      local ingestTarball="$( find "${DW_DATAWAVE_SOURCE_DIR}" -path "${DW_DATAWAVE_INGEST_TARBALL}" -type f | tail -1 )";
      ( cp "${ingestTarball}" "${DW_DATAWAVE_SERVICE_DIR}" ) || error "Failed to copy ingest tar file"
   fi
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
   datawaveIsInstalled && info "Uninstalling DataWave" && datawaveUninstall

   # Now all we need to do is remove the current DW tarballs and reset the environment
   rm -f ${DW_DATAWAVE_SERVICE_DIR}/datawave*.tar.gz

   export DW_REDEPLOY_IN_PROGRESS=true
   resetDataWaveEnvironment && datawaveInstall
   export DW_REDEPLOY_IN_PROGRESS=false
}
