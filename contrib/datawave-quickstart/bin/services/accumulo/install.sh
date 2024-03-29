#!/usr/bin/env bash

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

# shellcheck source=../../env.sh
source "${BIN_DIR}/env.sh"
# shellcheck source=./bootstrap.sh
source "${THIS_DIR}/bootstrap.sh"
# shellcheck source=../hadoop/bootstrap.sh
source "${SERVICES_DIR}/hadoop/bootstrap.sh"

hadoopIsInstalled || fatal "Accumulo requires that Hadoop be installed"

verifyChecksum "${DW_ACCUMULO_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" "${DW_ACCUMULO_DIST_SHA512_CHECKSUM}"
verifyChecksum "${DW_ZOOKEEPER_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" "${DW_ZOOKEEPER_DIST_SHA512_CHECKSUM}"

if zookeeperIsInstalled ; then
   info "ZooKeeper is already installed"
else
   [ -f "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_DIST}" ] || fatal "ZooKeeper tarball not found"
   mkdir "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" || fatal "Failed to create ZooKeeper base directory"
   # Extract ZooKeeper, set symlink, and verify...
   tar xf "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_DIST}" -C "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" --strip-components=1 || fatal "Failed to extract ZooKeeper tarball"
   ( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/accumulo/${DW_ZOOKEEPER_BASEDIR}" "${DW_ZOOKEEPER_SYMLINK}" ) || fatal "Failed to set ZooKeeper symlink"

   zookeeperIsInstalled || fatal "ZooKeeper was not installed"
fi

accumuloIsInstalled && info "Accumulo is already installed" && exit 1

[ -f "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_DIST}" ] || fatal "Accumulo tarball not found"
mkdir "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" || fatal "Failed to create Accumulo base directory"
# Extract Accumulo, set symlink, and verify...
tar xf "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_DIST}" -C "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" --strip-components=1 || fatal "Failed to extract Accumulo tarball"
( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/accumulo/${DW_ACCUMULO_BASEDIR}" "${DW_ACCUMULO_SYMLINK}" ) || fatal "Failed to set Accumulo symlink"

accumuloIsInstalled || fatal "Accumulo was not installed"

info "Accumulo and ZooKeeper tarballs extracted and symlinked"

DW_ZOOKEEPER_CONF_DIR="${ZOOKEEPER_HOME}/conf"
DW_ACCUMULO_CONF_DIR="${ACCUMULO_HOME}/conf"

# Create default accumulo config
"${ACCUMULO_HOME}/bin/accumulo-cluster" create-config

# Create accumulo.properties from DW_ACCUMULO_PROPERTIES...
if [ -n "${DW_ACCUMULO_PROPERTIES}" ] ; then
   echo "${DW_ACCUMULO_PROPERTIES}" > "${DW_ACCUMULO_CONF_DIR}/accumulo.properties"
   info "Accumulo accumulo.properties written"
else
   warn "No accumulo.properties content defined! :("
fi

if [ -n "${DW_ACCUMULO_CLIENT_CONF}" ] ; then
   echo "${DW_ACCUMULO_CLIENT_CONF}" > "${DW_ACCUMULO_CONF_DIR}/accumulo-client.properties"
   info "Accumulo accumulo-client.properties written"
else
   warn "No accumulo-client.properties content defined! :("
fi

assertCreateDir "${DW_ACCUMULO_JVM_HEAPDUMP_DIR}"

# Update tserver and other options in accumulo-env.sh
sed -i'' -e "s~\(ACCUMULO_TSERVER_OPTS=\).*$~\1\"${DW_ACCUMULO_TSERVER_OPTS}\"~g" "${DW_ACCUMULO_CONF_DIR}/accumulo-env.sh"
sed -i'' -e "s~\(export JAVA_HOME=\).*$~\1\"${JAVA_HOME}\"~g" "${DW_ACCUMULO_CONF_DIR}/accumulo-env.sh"
sed -i'' -e "s~\(export ACCUMULO_MONITOR_OPTS=\).*$~\1\"\${POLICY} -Xmx2g -Xms512m\"~g" "${DW_ACCUMULO_CONF_DIR}/accumulo-env.sh"

# Update Accumulo bind host if it's not set to localhost
if [ "${DW_ACCUMULO_BIND_HOST}" != "localhost" ] ; then
  sed -i'' -e "s/localhost/${DW_ACCUMULO_BIND_HOST}/g" ${DW_ACCUMULO_CONF_DIR}/cluster.yaml
fi

# Write zoo.cfg file using our settings in DW_ZOOKEEPER_CONF
if [ -n "${DW_ZOOKEEPER_CONF}" ] ; then
   echo "${DW_ZOOKEEPER_CONF}" > "${DW_ZOOKEEPER_CONF_DIR}/zoo.cfg" || fatal "Failed to write zoo.cfg"
else
   warn "No zoo.cfg content defined! :("
fi

if ! hadoopIsRunning ; then
   info "Starting Hadoop, so that we can initialize Accumulo"
   hadoopStart
fi

if ! zookeeperIsRunning ; then
   info "Starting ZooKeeper, so that we can initialize Accumulo"
   zookeeperStart
fi

# Create VFS classpath directories
if [ -n "${DW_ACCUMULO_VFS_DATAWAVE_DIR}" ] && [ "${DW_ACCUMULO_VFS_DATAWAVE_ENABLED}" != false ] ; then
   "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p "${DW_ACCUMULO_VFS_DATAWAVE_DIR}" || fatal "Failed to create ${DW_ACCUMULO_VFS_DATAWAVE_DIR}"
fi

# Initialize Accumulo
"${ACCUMULO_HOME}/bin/accumulo" init \
 --clear-instance-name \
 --instance-name "${DW_ACCUMULO_INSTANCE_NAME}" \
 --password "${DW_ACCUMULO_PASSWORD}" || fatal "Failed to initialize Accumulo"

echo
info "Accumulo initialized and ready to start..."
echo
echo "      Start command: accumuloStart"
echo "       Stop command: accumuloStop"
echo "     Status command: accumuloStatus"
echo
info "See \$DW_CLOUD_HOME/bin/services/accumulo/bootstrap.sh to view/edit commands as needed"
echo
