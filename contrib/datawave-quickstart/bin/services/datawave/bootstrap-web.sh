
# You may override DW_WILDFLY_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_WILDFLY_DIST_URI="${DW_WILDFLY_DIST_URI:-https://download.jboss.org/wildfly/17.0.1.Final/wildfly-17.0.1.Final.tar.gz}"
DW_WILDFLY_DIST="$( downloadTarball "${DW_WILDFLY_DIST_URI}" "${DW_DATAWAVE_SERVICE_DIR}" && echo "${tarball}" )"
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

getDataWaveTarball "${DW_DATAWAVE_WEB_TARBALL}"
DW_DATAWAVE_WEB_DIST="${tarball}"

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

function datawaveWebDisplayBinaryInfo() {
  echo "Source: DataWave Web Version $(getDataWaveVersion)//$(datawaveWebTarballName)"
  local installedTarball="$(ls -1 ${DW_DATAWAVE_SERVICE_DIR}/$(basename ${DW_DATAWAVE_WEB_TARBALL}) 2>/dev/null)"
  if [[ -n "${installedTarball}" ]]; then
     echo " Local: ${installedTarball}"
  else
     echo " Local: Not loaded"
  fi
  echo "Source: ${DW_WILDFLY_DIST_URI}"
  local tarballName="$(basename "$DW_WILDFLY_DIST_URI")"
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