# Sourced by env.sh

#
# You should consider modifying these OS settings:
#
#   - Set system swappiness to 10 or below. Default is usually 60
#       which Accumulo will definitely complain about.
#
#   - Set max open files ('ulimit -n') to around 33K. Default is
#       1K which is too low in most cases
#

DW_ACCUMULO_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Zookeeper config

# You may override DW_ZOOKEEPER_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
# DW_ZOOKEEPER_DIST_URI should, if possible, be using https. There are potential security risks by using http.
DW_ZOOKEEPER_VERSION="3.7.2"
DW_ZOOKEEPER_DIST_URI="${DW_ZOOKEEPER_DIST_URI:-https://dlcdn.apache.org/zookeeper/zookeeper-${DW_ZOOKEEPER_VERSION}/apache-zookeeper-${DW_ZOOKEEPER_VERSION}-bin.tar.gz}"
# The sha512 checksum for the tarball. Value should be the hash value only and does not include the file name. Cannot be left blank.
DW_ZOOKEEPER_DIST_SHA512_CHECKSUM="${DW_ZOOKEEPER_DIST_SHA512_CHECKSUM:-6afbfc1afc8b9370281bd9862f37dbb1cb95ec54bb2ed4371831aa5c0f08cfee775050bd57ce5fc0836e61af27eed9f0076f54b98997dd0e15159196056e52ea}"
# shellcheck disable=SC2154
# shellcheck disable=SC2034
DW_ZOOKEEPER_DIST="$( { downloadTarball "${DW_ZOOKEEPER_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" || downloadMavenTarball "datawave-parent" "gov.nsa.datawave.quickstart" "zookeeper" "${DW_ZOOKEEPER_VERSION}" "${DW_ACCUMULO_SERVICE_DIR}"; } && echo "${tarball}" )"
DW_ZOOKEEPER_BASEDIR="zookeeper-install"
DW_ZOOKEEPER_SYMLINK="zookeeper"

# You may override DW_BIND_HOST in your env ahead of time, if needed
DW_BIND_HOST="${DW_BIND_HOST:-localhost}"

# If we are configured to bind to all interfaces, instead bind to the hostname
DW_ACCUMULO_BIND_HOST="${DW_ACCUMULO_BIND_HOST:-${DW_BIND_HOST}}"
if [ "$DW_ACCUMULO_BIND_HOST" == "0.0.0.0" ] ; then
  DW_ACCUMULO_BIND_HOST="$(hostname)"
fi

# zoo.cfg...
# shellcheck disable=SC2034
DW_ZOOKEEPER_CONF="
tickTime=2000
syncLimit=5
clientPort=2181
dataDir=${DW_CLOUD_DATA}/zookeeper
maxClientCnxns=100
4lw.commands.whitelist=ruok,wchs
admin.serverPort=8089
admin.enableServer=false"

# Accumulo config

# You may override DW_ACCUMULO_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
# DW_ACCUMULO_DIST_URI should, if possible, be using https. There are potential security risks by using http.
DW_ACCUMULO_VERSION="2.1.3"
DW_ACCUMULO_DIST_URI="${DW_ACCUMULO_DIST_URI:-https://dlcdn.apache.org/accumulo/${DW_ACCUMULO_VERSION}/accumulo-${DW_ACCUMULO_VERSION}-bin.tar.gz}"
# The sha512 checksum for the tarball. Value should be the hash value only and does not include the file name. Cannot be left blank.
DW_ACCUMULO_DIST_SHA512_CHECKSUM="${DW_ACCUMULO_DIST_SHA512_CHECKSUM:-1a27a144dc31f55ccc8e081b6c1bc6cc0362a8391838c53c166cb45291ff8f35867fd8e4729aa7b2c540f8b721f8c6953281bf589fc7fe320e4dc4d20b87abc4}"
# shellcheck disable=SC2034
DW_ACCUMULO_DIST="$( { downloadTarball "${DW_ACCUMULO_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" || downloadMavenTarball "datawave-parent" "gov.nsa.datawave.quickstart" "accumulo" "${DW_ACCUMULO_VERSION}" "${DW_ACCUMULO_SERVICE_DIR}"; } && echo "${tarball}" )"
DW_ACCUMULO_BASEDIR="accumulo-install"
DW_ACCUMULO_SYMLINK="accumulo"
DW_ACCUMULO_INSTANCE_NAME="my-instance-01"
DW_ACCUMULO_PASSWORD="${DW_ACCUMULO_PASSWORD:-secret}"

alias ashell="accumulo shell -u root -p \${DW_ACCUMULO_PASSWORD}"

# Note that example configuration is provided for setting up VFS classpath for DataWave jars,
# but it is disabled by default, as it doesn't really buy you anything on a standalone cluster.
# To enable, set DW_ACCUMULO_VFS_DATAWAVE_ENABLED to true. If enabled, just be aware that
# writing all the DataWave jars to HDFS will probably slow down your install significantly

DW_ACCUMULO_VFS_DATAWAVE_ENABLED=${DW_ACCUMULO_VFS_DATAWAVE_ENABLED:-false}
DW_ACCUMULO_VFS_DATAWAVE_DIR="/datawave/accumulo-vfs-classpath"

# accumulo.properties (Format: <property-name>=<property-value>{<newline>})

DW_ACCUMULO_PROPERTIES="## Sets location in HDFS where Accumulo will store data
instance.volumes=${DW_HADOOP_DFS_URI_CLIENT}/accumulo

## Sets location of Zookeepers
instance.zookeeper.host=localhost:2181

## Change secret before initialization. All Accumulo servers must have same secret
instance.secret=${DW_ACCUMULO_PASSWORD}

## Set to false if 'accumulo-util build-native' fails
tserver.memory.maps.native.enabled=false
tserver.memory.maps.max=385M
tserver.cache.data.size=64M
tserver.cache.index.size=64M

## Trace user
trace.user=root

## Trace password
trace.password=${DW_ACCUMULO_PASSWORD}"

if [ "${DW_ACCUMULO_VFS_DATAWAVE_ENABLED}" != false ] ; then
  DW_ACCUMULO_PROPERTIES="${DW_ACCUMULO_PROPERTIES}
general.vfs.context.classpath.datawave=${DW_HADOOP_DFS_URI_CLIENT}${DW_ACCUMULO_VFS_DATAWAVE_DIR}/.*.jar"
else
  DW_ACCUMULO_PROPERTIES="${DW_ACCUMULO_PROPERTIES}
general.vfs.context.classpath.extlib=file://${ACCUMULO_HOME}/lib/ext/.*.jar"
fi

# shellcheck disable=SC2034
DW_ACCUMULO_CLIENT_CONF="instance.name=${DW_ACCUMULO_INSTANCE_NAME}
instance.zookeepers=localhost:2181
auth.type=password
auth.principal=root
auth.token=${DW_ACCUMULO_PASSWORD}"

DW_ACCUMULO_JVM_HEAPDUMP_DIR="${DW_CLOUD_DATA}/heapdumps"

# shellcheck disable=SC2034

export ZOOKEEPER_HOME="${DW_CLOUD_HOME}/${DW_ZOOKEEPER_SYMLINK}"
export ACCUMULO_HOME="${DW_CLOUD_HOME}/${DW_ACCUMULO_SYMLINK}"
export PATH=${ACCUMULO_HOME}/bin:${ZOOKEEPER_HOME}/bin:$PATH

# Service helper variables and functions....

DW_ZOOKEEPER_CMD_START="( cd ${ZOOKEEPER_HOME}/bin && ./zkServer.sh start )"
DW_ZOOKEEPER_CMD_STOP="( cd ${ZOOKEEPER_HOME}/bin && ./zkServer.sh stop )"
DW_ZOOKEEPER_CMD_FIND_ALL_PIDS="ps -ef | grep 'zookeeper.server.quorum.QuorumPeerMain' | grep -v grep | awk '{ print \$2 }'"

DW_ACCUMULO_CMD_START="( cd ${ACCUMULO_HOME}/bin && ./accumulo-cluster start )"
DW_ACCUMULO_CMD_STOP="( cd ${ACCUMULO_HOME}/bin && ./accumulo-cluster stop )"
DW_ACCUMULO_CMD_FIND_ALL_PIDS="pgrep -u ${USER} -d ' ' -f 'o.start.Main manager|o.start.Main tserver|o.start.Main monitor|o.start.Main gc|o.start.Main tracer'"

function accumuloIsRunning() {
    DW_ACCUMULO_PID_LIST="$(eval "${DW_ACCUMULO_CMD_FIND_ALL_PIDS}")"

    zookeeperIsRunning

    [[ -z "${DW_ACCUMULO_PID_LIST}" && -z "${DW_ZOOKEEPER_PID_LIST}" ]] && return 1 || return 0
}

function accumuloStart() {
    accumuloIsRunning && echo "Accumulo is already running" && return 1

    if ! zookeeperIsRunning ; then
       zookeeperStart
       echo
    fi
    if ! hadoopIsRunning ; then
       hadoopStart
       echo
    fi
    eval "${DW_ACCUMULO_CMD_START}"
    echo
    info "For detailed status visit 'http://${DW_ACCUMULO_BIND_HOST}:9995' in your browser"
}

function accumuloStop() {
    accumuloIsRunning && [ -n "${DW_ACCUMULO_PID_LIST}" ] && eval "${DW_ACCUMULO_CMD_STOP}" || echo "Accumulo is already stopped"
    zookeeperStop
}

function accumuloStatus() {
    # define vars for accumulo processes
    local _gc
    local _manager
    local _monitor
    local _tracer
    local _tserver

    echo "======  Accumulo Status  ======"
    local _opt=pid
    local _arg

    accumuloIsRunning
    test -n "${DW_ACCUMULO_PID_LIST}" && {
        local -r _pids=${DW_ACCUMULO_PID_LIST// /|}
        echo "pids: ${DW_ACCUMULO_PID_LIST}"

        for _arg in $(jps -lm | grep -E "${_pids}"); do

            case ${_opt} in
                pid)
                    _pid=${_arg}
                    _opt=class;;
                class) _opt=component;;
                component)
                    local _none
                    case "${_arg}" in
                        gc) _gc=${_pid};;
                        manager) _manager=${_pid};;
                        monitor) _monitor=${_pid};;
                        tracer) _tracer=${_pid};;
                        tserver) _tserver=${_pid};;
                        *) _none=true;;
                    esac

                    test -z "${_none}" && info "${_arg} => ${_pid}"
                    _opt=pid
                    unset _none
                    _pid=;;
            esac
        done
    }

    test -z "${_gc}" && warn "gc is not running"
    test -z "${_manager}" && warn "manager is not running"
    test -z "${_monitor}" && info "monitor is not running"
    test -z "${_tracer}" && info "tracer is not running"
    test -z "${_tserver}" && warn "tserver is not running"

    echo "======  ZooKeeper Status  ======"
    if [[ -n "${DW_ZOOKEEPER_PID_LIST}" ]]; then
        info "ZooKeeper => ${DW_ZOOKEEPER_PID_LIST}"
    else
        warn "ZooKeeper is not running"
    fi
}

function accumuloUninstall() {
    # Remove accumulo
    if accumuloIsInstalled ; then
       if [ -L "${DW_CLOUD_HOME}/${DW_ACCUMULO_SYMLINK}" ] ; then
           ( cd "${DW_CLOUD_HOME}" && unlink "${DW_ACCUMULO_SYMLINK}" ) || error "Failed to remove Accumulo symlink"
       fi

       if [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" ] ; then
           rm -rf "${DW_ACCUMULO_SERVICE_DIR:?}/${DW_ACCUMULO_BASEDIR}"
       fi

       # shellcheck disable=SC2015
       ! accumuloIsInstalled && info "Accumulo uninstalled" || error "Failed to uninstall Accumulo"
    else
      info "Accumulo not installed. Nothing to do"
    fi

    # Remove zookeeper
    if zookeeperIsInstalled ; then
       if [ -L "${DW_CLOUD_HOME}/${DW_ZOOKEEPER_SYMLINK}" ] ; then
           ( cd "${DW_CLOUD_HOME}" && unlink "${DW_ZOOKEEPER_SYMLINK}" ) || error "Failed to remove ZooKeeper symlink"
       fi

       if [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" ] ; then
           rm -rf "${DW_ACCUMULO_SERVICE_DIR:?}/${DW_ZOOKEEPER_BASEDIR}"
       fi

       # shellcheck disable=SC2015
       ! zookeeperIsInstalled && info "ZooKeeper uninstalled" || error "Failed to uninstall ZooKeeper"
    else
       info "ZooKeeper not installed. Nothing to do"
    fi

    [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_ACCUMULO_SERVICE_DIR}"/*.tar.gz
}

function accumuloInstall() {
  "${DW_ACCUMULO_SERVICE_DIR}/install.sh"
}

function zookeeperIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_ZOOKEEPER_SYMLINK}" ] && return 0
    [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" ] && return 0
    return 1
}

function accumuloIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_ACCUMULO_SYMLINK}" ] && return 0
    [ -d "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" ] && return 0
    return 1
}

function zookeeperIsRunning() {
    DW_ZOOKEEPER_PID_LIST="$(eval "${DW_ZOOKEEPER_CMD_FIND_ALL_PIDS}")"
    [ -z "${DW_ZOOKEEPER_PID_LIST}" ] && return 1 || return 0
}

function zookeeperStart() {
    # shellcheck disable=SC2015
    zookeeperIsRunning && echo "ZooKeeper is already running" || eval "${DW_ZOOKEEPER_CMD_START}"
}

function zookeeperStop() {
    zookeeperIsRunning && eval "${DW_ZOOKEEPER_CMD_STOP}" || echo "ZooKeeper is already stopped"
}

function zookeeperStatus() {
    zookeeperIsRunning && echo "ZooKeeper is running. PIDs: ${DW_ZOOKEEPER_PID_LIST}" || echo "ZooKeeper is not running"
}

function accumuloPrintenv() {
   echo
   echo "Accumulo Environment"
   echo
   ( set -o posix ; set ) | grep -E "ACCUMULO_|ZOOKEEPER_"
   echo
}

function accumuloPidList() {
   # Refresh pid lists
   accumuloIsRunning
   zookeeperIsRunning
   if [[ -n "${DW_ACCUMULO_PID_LIST}" || -n "${DW_ZOOKEEPER_PID_LIST}" ]] ; then
      echo "${DW_ACCUMULO_PID_LIST} ${DW_ZOOKEEPER_PID_LIST}"
   fi
}

function accumuloDisplayBinaryInfo() {
  echo "Source: ${DW_ACCUMULO_DIST}"
  local tarballName="$(basename "$DW_ACCUMULO_DIST")"
  if [[ -f "${DW_ACCUMULO_SERVICE_DIR}/${tarballName}" ]]; then
     echo " Local: ${DW_ACCUMULO_SERVICE_DIR}/${tarballName}"
  else
     echo " Local: Not loaded"
  fi
  echo "Source: ${DW_ZOOKEEPER_DIST}"
  tarballName="$(basename "$DW_ZOOKEEPER_DIST")"
  if [[ -f "${DW_ACCUMULO_SERVICE_DIR}/${tarballName}" ]]; then
     echo " Local: ${DW_ACCUMULO_SERVICE_DIR}/${tarballName}"
  else
     echo " Local: Not loaded"
  fi
}
