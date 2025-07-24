# Sourced by env.sh

DW_NIFI_SERVICE_DIR="$( dirname "${BASH_SOURCE[0]}" )"

DW_NIFI_DIST_URI="${DW_NIFI_DIST_URI:-https://dlcdn.apache.org/nifi/1.25.0/nifi-1.25.0-bin.zip}"
DW_NIFI_DIST_SHA512_CHECKSUM="${DW_NIFI_DIST_SHA512_CHECKSUM:-3798e8923cfc9099b785ee2019e9a0fe8bcd36301946f19d21d414800ca6b7fedd1bbe28764fa446262a2f47b1c608651208c8d8790c73bea9ebd839f42dbab1}"
DW_NIFI_DIST="$( basename "${DW_NIFI_DIST_URI}" )"
DW_NIFI_BASEDIR="nifi-install"
DW_NIFI_SYMLINK="nifi"

# Standard exports...
export NIFI_HOME="${DW_CLOUD_HOME}/${DW_NIFI_SYMLINK}"
export PATH=${NIFI_HOME}/bin:$PATH

# Service helpers...

DW_NIFI_CMD_START="( cd ${NIFI_HOME}/bin && ./nifi.sh start )"
DW_NIFI_CMD_STOP="( cd ${NIFI_HOME}/bin && ./nifi.sh stop )"
DW_NIFI_CMD_FIND_ALL_PIDS="pgrep -u ${USER} -d ' ' -f 'org.apache.nifi'"

function bootstrapNifi() {
    if [ ! -f "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_DIST}" ]; then
        info "Nifi distribution not detected. Attempting to bootstrap a dedicated install..."
        downloadTarball "${DW_NIFI_DIST_URI}" "${DW_NIFI_SERVICE_DIR}" || \
          ( fatal "failed to obtain Nifi distribution" && return 1 )
        DW_NIFI_DIST="${tarball}"
    else
      info "Nifi distribution detected. Using local file ${DW_NIFI_DIST}"
    fi
}

function nifiIsRunning() {
    DW_NIFI_PID_LIST="$(eval "${DW_NIFI_CMD_FIND_ALL_PIDS}")"
    [ -z "${DW_NIFI_PID_LIST}" ] && return 1 || return 0
}

function nifiStart() {
    nifiIsRunning && echo "NiFi is already running" || eval "${DW_NIFI_CMD_START}" || return 1
    info "To get to the UI, visit 'http://localhost:8080/nifi/' in your browser"
    info "Be patient, it may take a while for the NiFi web service to start"
}

function nifiStop() {
    nifiIsRunning && eval "${DW_NIFI_CMD_STOP}" || echo "NiFi is already stopped"
}

function nifiStatus() {
    nifiIsRunning && echo "NiFi is running. PIDs: ${DW_NIFI_PID_LIST}" || echo "NiFi is not running"
}

function nifiIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_NIFI_SYMLINK}" ] && return 0
    [ -d "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}" ] && return 0
    return 1
}

function nifiUninstall() {
   if nifiIsInstalled ; then
       if [ -L "${DW_CLOUD_HOME}/${DW_NIFI_SYMLINK}" ] ; then
           ( cd "${DW_CLOUD_HOME}" && unlink "${DW_NIFI_SYMLINK}" ) || error "Failed to remove NiFi symlink"
       fi

       if [ -d "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}" ] ; then
           rm -rf "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}"
       fi

       ! nifiIsInstalled && info "NiFi uninstalled" || error "Failed to uninstall NiFi"
   else
      info "NiFi not installed. Nothing to do"
   fi

   [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_NIFI_SERVICE_DIR}"/*.tar.gz
}

function nifiInstall() {
   ${DW_NIFI_SERVICE_DIR}/install.sh
      return_code=$?
      # Check the return value
      if [ $return_code -eq 0 ]; then
          echo "nifi install.sh executed successfully."
          return 0
      else
          echo "nifi install.sh failed with exit status: $return_code"
          return $return_code
      fi
}

function nifiPidList() {

   nifiIsRunning && echo "${DW_NIFI_PID_LIST}"

}

function nifiPrintenv() {
   echo
   echo "NiFi Environment"
   echo
   ( set -o posix ; set ) | grep NIFI_
   echo
}
