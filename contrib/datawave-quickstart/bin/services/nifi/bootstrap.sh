# Sourced by env.sh

DW_NIFI_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# You may override DW_NIFI_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
# DW_NIFI_DIST_URI should, if possible, be using https. There are potential security risks by using http.
DW_NIFI_DIST_URI="${DW_NIFI_DIST_URI:-https://dlcdn.apache.org/nifi/1.25.0/nifi-1.25.0-bin.zip}"
# The sha512 checksum for the tarball. Value should be the hash value only and does not include the file name. Cannot be left blank.
DW_NIFI_DIST_SHA512_CHECKSUM="${DW_NIFI_DIST_SHA512_CHECKSUM:-3798e8923cfc9099b785ee2019e9a0fe8bcd36301946f19d21d414800ca6b7fedd1bbe28764fa446262a2f47b1c608651208c8d8790c73bea9ebd839f42dbab1}"
DW_NIFI_DIST="$( downloadTarball "${DW_NIFI_DIST_URI}" "${DW_NIFI_SERVICE_DIR}" && echo "${tarball}" )"
DW_NIFI_BASEDIR="nifi-install"
DW_NIFI_SYMLINK="nifi"

# Standard exports...
export NIFI_HOME="${DW_CLOUD_HOME}/${DW_NIFI_SYMLINK}"
export PATH=${NIFI_HOME}/bin:$PATH

# Service helpers...

DW_NIFI_CMD_START="( cd ${NIFI_HOME}/bin && ./nifi.sh start )"
DW_NIFI_CMD_STOP="( cd ${NIFI_HOME}/bin && ./nifi.sh stop )"
DW_NIFI_CMD_FIND_ALL_PIDS="pgrep -u ${USER} -d ' ' -f 'org.apache.nifi'"

function nifiIsRunning() {
    DW_NIFI_PID_LIST="$(eval "${DW_NIFI_CMD_FIND_ALL_PIDS}")"
    [ -z "${DW_NIFI_PID_LIST}" ] && return 1 || return 0
}

function nifiStart() {
    nifiIsRunning && echo "NiFi is already running" || eval "${DW_NIFI_CMD_START}"
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
