
# Get these vars from the pom so users not building the container image can stay up to date
DW_WILDFLY_VERSION="${DW_WILDFLY_VERSION:-$(mvn -q -f ${DW_CLOUD_HOME}/docker/pom.xml help:evaluate -DforceStdout -Dexpression=version.quickstart.wildfly | tail -1)}"
DW_WILDFLY_DIST_SHA512_CHECKSUM="${DW_WILDFLY_DIST_SHA512_CHECKSUM:-$(mvn -q -f ${DW_CLOUD_HOME}/docker/pom.xml help:evaluate -DforceStdout -Dexpression=sha512.checksum.wildfly | tail -1)}"

DW_WILDFLY_DIST_URI="${DW_WILDFLY_DIST_URI:-https://download.jboss.org/wildfly/${DW_WILDFLY_VERSION}.Final/wildfly-${DW_WILDFLY_VERSION}.Final.tar.gz}"
DW_WILDFLY_DIST="$( basename "${DW_WILDFLY_DIST_URI}" )"
DW_WILDFLY_BASEDIR="wildfly-install"
DW_WILDFLY_SYMLINK="wildfly"

export WILDFLY_HOME="$(readlink -f ${DW_CLOUD_HOME}/${DW_WILDFLY_SYMLINK})"
export JBOSS_HOME="${WILDFLY_HOME}"
export PATH="${WILDFLY_HOME}/bin:${PATH}"
export DW_DATAWAVE_WEB_JAVA_OPTS=${DW_DATAWAVE_WEB_JAVA_OPTS:-"-Duser.timezone=GMT -Dfile.encoding=UTF-8 -Djava.net.preferIPv4Stack=true"}

DW_DATAWAVE_WEB_CMD_START="( cd "${WILDFLY_HOME}/bin" && JAVA_OPTS=\"$DW_DATAWAVE_WEB_JAVA_OPTS\" nohup ./standalone.sh -c standalone-full.xml & )"
DW_DATAWAVE_WEB_CMD_START_DEBUG="( cd "${WILDFLY_HOME}/bin" && JAVA_OPTS=\"$DW_DATAWAVE_WEB_JAVA_OPTS\" nohup ./standalone.sh --debug -c standalone-full.xml & )"
DW_DATAWAVE_WEB_CMD_STOP="datawaveWebIsRunning && [[ ! -z \$DW_DATAWAVE_WEB_PID_LIST ]] && kill -15 \$DW_DATAWAVE_WEB_PID_LIST"

DW_DATAWAVE_WEB_CMD_FIND_ALL_PIDS="pgrep -d ' ' -f 'jboss.home.dir=${DW_CLOUD_HOME}/.*wildfly'"

DW_DATAWAVE_WEB_SYMLINK="datawave-webservice"
DW_DATAWAVE_WEB_BASEDIR="datawave-webservice-install"

# uncomment to enable environment passwords in the quickstart
# export DW_ACCUMULO_PASSWORD="secret"

function bootstrapWildfly() {
    if [ ! -f "${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_DIST}" ]; then
        info "Wildfly distribution not detected. Attempting to bootstrap a dedicated install..."
        downloadTarball "${DW_WILDFLY_DIST_URI}" "${DW_DATAWAVE_SERVICE_DIR}" || \
          downloadMavenTarball "datawave-parent" "gov.nsa.datawave.quickstart" "wildfly" "${DW_WILDFLY_VERSION}" "${DW_DATAWAVE_SERVICE_DIR}" || \
          ( fatal "failed to obtain Wildfly distribution" && return 1 )
        DW_WILDFLY_DIST="${tarball}"
    else
      info "Wildfly distribution detected. Using local file ${DW_WILDFLY_DIST}"
    fi
}

function datawaveWebIsRunning() {
    DW_DATAWAVE_WEB_PID_LIST="$(eval "${DW_DATAWAVE_WEB_CMD_FIND_ALL_PIDS}")"
    [ -z "${DW_DATAWAVE_WEB_PID_LIST}" ] && return 1 || return 0
}

function datawaveWebStop() {
    ! datawaveWebIsRunning && echo "DataWave Web is already stopped" && return 1

    while datawaveWebIsRunning ; do
        echo "Stopping Wildfly"
        eval "${DW_DATAWAVE_WEB_CMD_STOP}"
        sleep 5
        if datawaveWebIsRunning ; then
            warn "DataWave Web did not stop" && datawaveWebStatus
        else
            echo "DataWave Web stopped" && return 0
        fi
    done
}

function datawaveWebStatus() {
    echo "======  DataWave Web Status  ======"
    datawaveWebIsRunning && info "Wildfly is running => ${DW_DATAWAVE_WEB_PID_LIST}" || warn "Wildfly is not running"
}

function datawaveWebIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_WEB_SYMLINK}" ] && \
    [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_BASEDIR}" ] && \
    wildflyIsInstalled && return 0

    return 1
}

function wildflyIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_WILDFLY_SYMLINK}" ] && \
    [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_BASEDIR}" ] && return 0

    return 1
}

function datawaveWebTest() {
    "${DW_DATAWAVE_SERVICE_DIR}"/test-web/run.sh "$@"
}

function datawaveWebUninstall() {
   # Guard against false negative (bug) by testing to see if a "datawave-webservice-*" dir does actually exist, since
   # DW_DATAWAVE_WEB_BASEDIR value may have been only partially defined (without the DW version, e.g., if build failed, etc)
   # This is only for backward compatibility, since now the dir name is no longer defined dynamically
   local install_dir="$( find "${DW_DATAWAVE_SERVICE_DIR}" -maxdepth 1 -type d -name "${DW_DATAWAVE_WEB_BASEDIR}*" )"

   if datawaveWebIsInstalled || [ -n "${install_dir}" ] ; then

      if [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_WEB_SYMLINK}" ] ; then
          ( cd "${DW_CLOUD_HOME}" && unlink "${DW_DATAWAVE_WEB_SYMLINK}" ) || error "Failed to remove DataWave Web symlink"
      fi

      if [ -n "${install_dir}" ] ; then
          rm -rf "${install_dir}"
      fi

      if [ -L "${DW_CLOUD_HOME}/${DW_WILDFLY_SYMLINK}" ] ; then
          ( cd "${DW_CLOUD_HOME}" && unlink "${DW_WILDFLY_SYMLINK}" ) || error "Failed to remove Wildfly symlink"
      fi

      if [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_BASEDIR}" ] ; then
          rm -rf "${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_BASEDIR}"
      fi

      ! datawaveWebIsInstalled && info "DataWave Web uninstalled" || error "Failed to uninstall DataWave Web"

   else
      info "DataWave Web not installed. Nothing to do"
   fi
}

function datawaveWebInstall() {
   "${DW_DATAWAVE_SERVICE_DIR}"/install-web.sh
      return_code=$?
      # Check the return value
      if [ $return_code -eq 0 ]; then
          echo "datawave install-web.sh executed successfully."
          return 0
      else
          echo "datawave install-web.sh failed with exit status: $return_code"
          return $return_code
      fi
}

function datawaveWebIsDeployed() {
   if ! datawaveWebIsRunning ; then
      DW_DATAWAVE_EAR_STATUS="WILDFLY_DOWN"
      return 1
   fi
   local deployedOK="$( ${WILDFLY_HOME}/bin/jboss-cli.sh -c --command="deployment-info --name=datawave-ws-deploy-*.ear" | grep OK )"
   if [ -z "${deployedOK}" ] ; then
      DW_DATAWAVE_EAR_STATUS="DATAWAVE_EAR_NOT_DEPLOYED"
      return 1
   fi
   DW_DATAWAVE_EAR_STATUS="DATAWAVE_EAR_DEPLOYED"
   return 0
}

function datawaveWebStart() {

    local debug=false

    # Use --debug flag to start Wildfly in debug mode
    [[ "${1}" == "--debug" || "${1}" == "-d" ]] && debug=true
    [[ -n "${1}" && "${debug}" == false ]] && error "Unrecognized option: ${1}" && return

    hadoopIsRunning || hadoopStart || return 1
    accumuloIsRunning || accumuloStart || return 1

    if datawaveWebIsRunning ; then
       info "Wildfly is already running"
    else
       if [ "${debug}" == true ] ; then
           info "Starting Wildfly in debug mode"
           eval "${DW_DATAWAVE_WEB_CMD_START_DEBUG}" > /dev/null 2>&1 || return 1
       else
           info "Starting Wildfly"
           eval "${DW_DATAWAVE_WEB_CMD_START}" > /dev/null 2>&1 || return 1
       fi
    fi

    local pollInterval=4
    local maxAttempts=15

    info "Polling for EAR deployment status every ${pollInterval} seconds (${maxAttempts} attempts max)"

    for (( i=1; i<=${maxAttempts}; i++ ))
    do
       if datawaveWebIsDeployed ; then
          echo "    ++ DataWave Web successfully deployed (${i}/${maxAttempts})"
          echo
          info "Documentation: https://localhost:8443/DataWave/doc"
          info "Data Dictionary: https://localhost:8443/DataWave/DataDictionary"
          info "NOTE: DataDictionary will need to be deployed separately via the microservices modules in order to function."
          echo
          return 0
       fi
       case "${DW_DATAWAVE_EAR_STATUS}" in
          WILDFLY_DOWN)
             echo "    -- Wildfly process not found (${i}/${maxAttempts})"
             ;;
          DATAWAVE_EAR_NOT_DEPLOYED)
             echo "    +- Wildfly up (${DW_DATAWAVE_WEB_PID_LIST}). EAR deployment pending (${i}/${maxAttempts})"
             ;;
       esac
       sleep $pollInterval
    done
    return 1
}

function datawaveWebDisplayBinaryInfo() {
  echo "Source: DataWave Web Version $(getDataWaveVersion)//$(datawaveWebTarballName)"
  local installedTarball="$(ls -1 ${DW_DATAWAVE_SERVICE_DIR}/$(basename ${DW_DATAWAVE_WEB_TARBALL}) 2>/dev/null)"
  if [[ -n "${installedTarball}" ]]; then
     echo " Local: ${installedTarball}"
  else
     echo " Local: Not loaded"
  fi
  echo "Source: ${DW_WILDFLY_DIST}"
  local tarballName="$(basename "$DW_WILDFLY_DIST")"
  if [[ -f "${DW_DATAWAVE_SERVICE_DIR}/${tarballName}" ]]; then
     echo " Local: ${DW_DATAWAVE_SERVICE_DIR}/${tarballName}"
  else
     echo " Local: Not loaded"
  fi
}

function datawaveWebTarballName() {
   local dwVersion="$(getDataWaveVersion)"
   echo "$( basename "${DW_DATAWAVE_WEB_TARBALL/-\*-/-$dwVersion-}" )"
}
