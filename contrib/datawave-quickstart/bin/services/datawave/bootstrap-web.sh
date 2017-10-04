
# You may override DW_WILDFLY_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_WILDFLY_DIST_URI="${DW_WILDFLY_DIST_URI:-http://download.jboss.org/wildfly/10.1.0.Final/wildfly-10.1.0.Final.tar.gz}"
DW_WILDFLY_DIST="$( downloadTarball "${DW_WILDFLY_DIST_URI}" "${DW_DATAWAVE_SERVICE_DIR}" && echo "${tarball}" )"
DW_WILDFLY_BASEDIR="wildfly-install"
DW_WILDFLY_SYMLINK="wildfly"

export WILDFLY_HOME="${DW_CLOUD_HOME}/${DW_WILDFLY_SYMLINK}"
export JBOSS_HOME="${WILDFLY_HOME}"
export PATH="${WILDFLY_HOME}/bin:${PATH}"

DW_DATAWAVE_WEB_CMD_START="( cd "${WILDFLY_HOME}/bin" && nohup ./standalone.sh -c standalone-full.xml & )"
DW_DATAWAVE_WEB_CMD_START_DEBUG="( cd "${WILDFLY_HOME}/bin" && nohup ./standalone.sh --debug -c standalone-full.xml & )"
DW_DATAWAVE_WEB_CMD_STOP="datawaveWebIsRunning && [[ ! -z \$DW_DATAWAVE_WEB_PID_LIST ]] && kill -15 \$DW_DATAWAVE_WEB_PID_LIST"

DW_DATAWAVE_WEB_CMD_FIND_ALL_PIDS="pgrep -f 'jboss.home.dir=${DW_CLOUD_HOME}/${DW_WILDFLY_SYMLINK}'"

DW_DATAWAVE_WEB_SYMLINK="datawave-webservice"

getDataWaveTarball "${DW_DATAWAVE_WEB_TARBALL}"
DW_DATAWAVE_WEB_DIST="${tarball}"
DW_DATAWAVE_WEB_VERSION="$( echo "${DW_DATAWAVE_WEB_DIST}" | sed "s/.*\///" | sed "s/datawave-ws-deploy-application-//" | sed "s/-dev.tar.gz//" )"
DW_DATAWAVE_WEB_BASEDIR="datawave-web-${DW_DATAWAVE_WEB_VERSION}"

function datawaveWebIsRunning() {
    DW_DATAWAVE_WEB_PID_LIST="$(eval "${DW_DATAWAVE_WEB_CMD_FIND_ALL_PIDS} -d ' '")"
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
    datawaveWebIsRunning && echo "DataWave Web is running. PIDs: ${DW_DATAWAVE_WEB_PID_LIST}" || echo "DataWave Web is not running"
}

function datawaveWebIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_WEB_SYMLINK}" ] && return 0
    [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_BASEDIR}" ] && return 0

    [ -L "${DW_CLOUD_HOME}/${DW_WILDFLY_SYMLINK}" ] && return 0
    [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_BASEDIR}" ] && return 0

    return 1
}

function datawaveWebTest() {
    "${DW_DATAWAVE_SERVICE_DIR}"/test-web/run.sh $@
}

function datawaveWebUninstall() {
   if datawaveWebIsInstalled ; then

      if [ -L "${DW_CLOUD_HOME}/${DW_DATAWAVE_WEB_SYMLINK}" ] ; then
          ( cd "${DW_CLOUD_HOME}" && unlink "${DW_DATAWAVE_WEB_SYMLINK}" ) || error "Failed to remove DataWave Web symlink"
      fi

      if [ -d "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_BASEDIR}" ] ; then
          rm -rf "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_BASEDIR}"
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

    ! hadoopIsRunning && hadoopStart
    ! accumuloIsRunning && accumuloStart

    if datawaveWebIsRunning ; then
       info "Wildfly is already running"
    else
       if [ "${debug}" == true ] ; then
           info "Starting Wildfly in debug mode"
           eval "${DW_DATAWAVE_WEB_CMD_START_DEBUG}" > /dev/null 2>&1
       else
           info "Starting Wildfly"
           eval "${DW_DATAWAVE_WEB_CMD_START}" > /dev/null 2>&1
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