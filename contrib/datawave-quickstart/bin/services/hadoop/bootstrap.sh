# Sourced by env.sh

DW_HADOOP_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DW_HADOOP_VERSION="3.3.6"
# You may override DW_HADOOP_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
# DW_HADOOP_DIST_URI should, if possible, be using https. There are potential security risks by using http.
DW_HADOOP_DIST_URI="${DW_HADOOP_DIST_URI:-https://dlcdn.apache.org/hadoop/common/hadoop-${DW_HADOOP_VERSION}/hadoop-${DW_HADOOP_VERSION}.tar.gz}"
# The sha512 checksum for the tarball. Value should be the hash value only and does not include the file name. Cannot be left blank.
DW_HADOOP_DIST_SHA512_CHECKSUM="${DW_HADOOP_DIST_SHA512_CHECKSUM:-de3eaca2e0517e4b569a88b63c89fae19cb8ac6c01ff990f1ff8f0cc0f3128c8e8a23db01577ca562a0e0bb1b4a3889f8c74384e609cd55e537aada3dcaa9f8a}"
DW_HADOOP_DIST="$( { downloadTarball "${DW_HADOOP_DIST_URI}" "${DW_HADOOP_SERVICE_DIR}" || downloadMavenTarball "datawave-parent" "gov.nsa.datawave.quickstart" "hadoop" "${DW_HADOOP_VERSION}" "${DW_HADOOP_SERVICE_DIR}"; } && echo "${tarball}" )"
DW_HADOOP_BASEDIR="hadoop-install"
DW_HADOOP_SYMLINK="hadoop"

# You may override DW_BIND_HOST in your env ahead of time, if needed
DW_BIND_HOST="${DW_BIND_HOST:-localhost}"

DW_HADOOP_DFS_URI_SERVER="hdfs://${DW_BIND_HOST}:9000"
DW_HADOOP_DFS_URI_CLIENT="hdfs://${DW_BIND_HOST}:9000"
DW_HADOOP_MR_INTER_DIR="/jobhist/inter"
DW_HADOOP_MR_DONE_DIR="/jobhist/done"
DW_HADOOP_RESOURCE_MANAGER_ADDRESS_SERVER="${DW_BIND_HOST}:8050"
DW_HADOOP_RESOURCE_MANAGER_ADDRESS_CLIENT="${DW_BIND_HOST}:8050"

if [ "${DW_BIND_HOST}" == "0.0.0.0" ] ; then
    DW_HADOOP_DFS_URI_CLIENT="hdfs://localhost:9000"
    DW_HADOOP_RESOURCE_MANAGER_ADDRESS_CLIENT="localhost:8050"
fi

HADOOP_HOME="${DW_CLOUD_HOME}/${DW_HADOOP_SYMLINK}"

# core-site.xml (Format: <property-name><space><property-value>{<newline>})
DW_HADOOP_CORE_SITE_CONF="fs.defaultFS ${DW_HADOOP_DFS_URI_SERVER}
hadoop.tmp.dir file://${DW_CLOUD_DATA}/hadoop/tmp
io.compression.codecs org.apache.hadoop.io.compress.GzipCodec"

# hdfs-site.xml (Format: <property-name><space><property-value>{<newline>})
DW_HADOOP_HDFS_SITE_CONF="dfs.namenode.name.dir file://${DW_CLOUD_DATA}/hadoop/nn
dfs.namenode.secondary.http-address ${DW_BIND_HOST}
dfs.namenode.checkpoint.dir file://${DW_CLOUD_DATA}/hadoop/nnchk
dfs.datanode.data.dir file://${DW_CLOUD_DATA}/hadoop/dn
dfs.datanode.handler.count 10
dfs.datanode.synconclose true
dfs.replication 1"

DW_HADOOP_MR_HEAPDUMP_DIR="${DW_CLOUD_DATA}/heapdumps"
# mapred-site.xml (Format: <property-name><space><property-value>{<newline>})
DW_HADOOP_MAPRED_SITE_CONF="mapreduce.jobhistory.address ${DW_BIND_HOST}:8020
mapreduce.jobhistory.webapp.address ${DW_BIND_HOST}:8021
mapreduce.jobhistory.intermediate-done-dir ${DW_HADOOP_MR_INTER_DIR}
mapreduce.jobhistory.done-dir ${DW_HADOOP_MR_DONE_DIR}
mapreduce.map.memory.mb 2048
mapreduce.reduce.memory.mb 2048
mapreduce.map.java.opts -Xmx1024m -server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -XX:+ExitOnOutOfMemoryError -XX:HeapDumpPath=${DW_HADOOP_MR_HEAPDUMP_DIR}
mapreduce.reduce.java.opts -Xmx1792m -server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -XX:+ExitOnOutOfMemoryError -XX:HeapDumpPath=${DW_HADOOP_MR_HEAPDUMP_DIR}
mapreduce.framework.name yarn
yarn.app.mapreduce.am.env HADOOP_MAPRED_HOME=${HADOOP_HOME}
mapreduce.map.env HADOOP_MAPRED_HOME=${HADOOP_HOME}
mapreduce.reduce.env HADOOP_MAPRED_HOME=${HADOOP_HOME}"

# yarn-site.xml (Format: <property-name><space><property-value>{<newline>})
DW_HADOOP_YARN_SITE_CONF="yarn.resourcemanager.scheduler.address ${DW_BIND_HOST}:8030
yarn.resourcemanager.scheduler.class org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
yarn.resourcemanager.resource-tracker.address ${DW_BIND_HOST}:8025
yarn.resourcemanager.address ${DW_HADOOP_RESOURCE_MANAGER_ADDRESS_SERVER}
yarn.resourcemanager.admin.address ${DW_BIND_HOST}:8033
yarn.resourcemanager.webapp.address ${DW_BIND_HOST}:8088
yarn.nodemanager.local-dirs ${DW_CLOUD_DATA}/hadoop/yarn/local
yarn.nodemanager.log-dirs ${DW_CLOUD_DATA}/hadoop/yarn/log
yarn.nodemanager.aux-services mapreduce_shuffle
yarn.nodemanager.pmem-check-enabled false
yarn.nodemanager.vmem-check-enabled false
yarn.nodemanager.resource.memory-mb 6144
yarn.app.mapreduce.am.resource.mb 1024
yarn.log.server.url http://localhost:8021/jobhistory/logs"

# capacity-scheduler.xml (Format: <property-name><space><property-value>{<newline>})
DW_HADOOP_CAPACITY_SCHEDULER_CONF="yarn.scheduler.capacity.maximum-applications 10000
yarn.scheduler.capacity.maximum-am-resource-percent 0.1
yarn.scheduler.capacity.resource-calculator org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator
yarn.scheduler.capacity.root.queues default,bulkIngestQueue,liveIngestQueue
yarn.scheduler.capacity.root.default.capacity 20
yarn.scheduler.capacity.root.bulkIngestQueue.capacity 40
yarn.scheduler.capacity.root.liveIngestQueue.capacity 40
yarn.scheduler.capacity.root.default.user-limit-factor 0.2
yarn.scheduler.capacity.root.bulkIngestQueue.user-limit-factor 0.4
yarn.scheduler.capacity.root.liveIngestQueue.user-limit-factor 0.4
yarn.scheduler.capacity.root.default.maximum-capacity 100
yarn.scheduler.capacity.root.bulkIngestQueue.maximum-capacity 90
yarn.scheduler.capacity.root.liveIngestQueue.maximum-capacity 90
yarn.scheduler.capacity.root.default.state RUNNING
yarn.scheduler.capacity.root.bulkIngestQueue.state RUNNING
yarn.scheduler.capacity.root.liveIngestQueue.state RUNNING
yarn.scheduler.capacity.root.default.acl_submit_applications *
yarn.scheduler.capacity.root.default.acl_administer_queue *
yarn.scheduler.capacity.node-locality-delay 40"

# Hadoop standard exports...
export HADOOP_HOME
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export HADOOP_LOG_DIR="${HADOOP_HOME}/logs"
export YARN_LOG_DIR="${DW_CLOUD_DATA}/hadoop/yarn/log"
export HADOOP_YARN_HOME="${HADOOP_HOME}"
export HADOOP_MAPRED_HOME="${HADOOP_HOME}"
export HADOOP_PID_DIR="${DW_CLOUD_DATA}/hadoop/pids"

export PATH=${HADOOP_HOME}/bin:$PATH

# Service helpers...

DW_HADOOP_CMD_START="( cd ${HADOOP_HOME}/sbin && ./start-dfs.sh && ./start-yarn.sh && mapred --daemon start historyserver )"
DW_HADOOP_CMD_STOP="( cd ${HADOOP_HOME}/sbin && mapred --daemon stop historyserver && ./stop-yarn.sh && ./stop-dfs.sh )"
DW_HADOOP_CMD_FIND_ALL_PIDS="pgrep -u ${USER} -d ' ' -f 'proc_datanode|proc_namenode|proc_secondarynamenode|proc_nodemanager|proc_resourcemanager|mapreduce.v2.hs.JobHistoryServer'"

function hadoopIsRunning() {
    DW_HADOOP_PID_LIST="$(eval "${DW_HADOOP_CMD_FIND_ALL_PIDS}")"
    [ -z "${DW_HADOOP_PID_LIST}" ] && return 1 || return 0
}

function hadoopStart() {
    hadoopIsRunning && echo "Hadoop is already running" || eval "${DW_HADOOP_CMD_START}"
    echo
    info "For detailed status visit 'http://localhost:9870/dfshealth.html#tab-overview' in your browser"
    # Wait for Hadoop to come out of safemode
    ${HADOOP_HOME}/bin/hdfs dfsadmin -safemode wait
}

function hadoopStop() {
    hadoopIsRunning && eval "${DW_HADOOP_CMD_STOP}" || echo "Hadoop is already stopped"
}

function hadoopStatus() {
    # define local variables for hadoop processes
    local _jobHist
    local _dataNode
    local _nameNode
    local _secNameNode
    local _resourceMgr
    local _nodeMgr

    # use a state to parse jps entries
    echo "======  Hadoop Status  ======"
    hadoopIsRunning && {
        local _pid
        local _opt=pid

        local -r _pids=${DW_HADOOP_PID_LIST// /|}
        echo "pids: ${DW_HADOOP_PID_LIST}"
        for _arg in $(jps -l | egrep "${_pids}"); do
            case ${_opt} in
                pid)
                    _pid=${_arg}
                    _opt=class
                    ;;
                class)
                    local _none
                    local _name=${_arg##*.}
                    case "${_name}" in
                        DataNode) _dataNode=${_pid};;
                        JobHistoryServer) _jobHist=${_pid};;
                        NameNode) _nameNode=${_pid};;
                        NodeManager) _nodeMgr=${_pid};;
                        ResourceManager) _resourceMgr=${_pid};;
                        SecondaryNameNode) _secNameNode=${_pid};;
                        *) _none=true;;
                    esac
                    test -z "${_none}" && info "${_name} => ${_pid}"
                    _pid=
                    _opt=pid
                    unset _none
                    ;;
            esac
        done
    }

    test -z "${_jobHist}" && warn "JobHistoryServer is not running"
    test -z "${_dataNode}" && warn "DataNode is not running"
    test -z "${_nameNode}" && warn "NameNode is not running"
    test -z "${_secNameNode}" && warn "SecondaryNameNode is not running"
    test -z "${_nodeMgr}" && warn "NodeManager is not running"
    test -z "${_resourceMgr}" && warn "ResourceManager is not running"
}

function hadoopIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_HADOOP_SYMLINK}" ] && return 0
    [ -d "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_BASEDIR}" ] && return 0
    return 1
}

function hadoopUninstall() {
   if hadoopIsInstalled ; then
      if [ -L "${DW_CLOUD_HOME}/${DW_HADOOP_SYMLINK}" ] ; then
          ( cd "${DW_CLOUD_HOME}" && unlink "${DW_HADOOP_SYMLINK}" ) || error "Failed to remove Hadoop symlink"
      fi

      if [ -d "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_BASEDIR}" ] ; then
          rm -rf "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_BASEDIR}"
      fi

      ! hadoopIsInstalled && info "Hadoop uninstalled" || error "Failed to uninstall Hadoop"
   else
      info "Hadoop not installed. Nothing to do"
   fi

   [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_HADOOP_SERVICE_DIR}"/*.tar.gz
}

function hadoopInstall() {
  "${DW_HADOOP_SERVICE_DIR}"/install.sh
}

function hadoopPrintenv() {
   echo
   echo "Hadoop Environment"
   echo
   ( set -o posix ; set ) | grep HADOOP_
   echo
}

function hadoopPidList() {

   hadoopIsRunning && echo "${DW_HADOOP_PID_LIST}"

}

function hadoopDisplayBinaryInfo() {
  echo "Source: ${DW_HADOOP_DIST}"
  local tarballName="$(basename "$DW_HADOOP_DIST")"
  if [[ -f "${DW_HADOOP_SERVICE_DIR}/${tarballName}" ]]; then
     echo " Local: ${DW_HADOOP_SERVICE_DIR}/${tarballName}"
  else
     echo " Local: Not loaded"
  fi
}
