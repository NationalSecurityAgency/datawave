#!/usr/bin/env bash
set -e

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

# if JDK is not installed exit early
jdkIsConfigured

hadoopIsInstalled || ( fatal "Accumulo requires that Hadoop be installed" && exit 1 )

# it might not be installed and we do not want to fail so we check with a traditional if statement
if accumuloIsInstalled && zookeeperIsInstalled; then
    info "Accumulo and Zookeeper are already installed"
    exit 0
fi

# If Zookeeper is not installed, bootstrap and verify that the two checksums match before installing.
if ! zookeeperIsInstalled ; then
    bootstrapZookeeper
    verifyChecksum "${DW_ZOOKEEPER_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" "${DW_ZOOKEEPER_DIST_SHA512_CHECKSUM}" || exit 1

    info "Installing ZooKeeper..."
    [ -f "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_DIST}" ] || ( fatal "ZooKeeper tarball not found" && exit 1 )
    mkdir "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" || ( fatal "Failed to create ZooKeeper base directory" && exit 1 )
    # Extract ZooKeeper, set symlink, and verify...
    tar xf "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_DIST}" -C "${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}" --strip-components=1 || ( fatal "Failed to extract ZooKeeper tarball" && exit 1 )
    #symlink the zookeeper jars if needed
    ln -s ${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}/lib/* ${DW_ACCUMULO_SERVICE_DIR}/${DW_ZOOKEEPER_BASEDIR}
    ( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/accumulo/${DW_ZOOKEEPER_BASEDIR}" "${DW_ZOOKEEPER_SYMLINK}" ) || ( fatal "Failed to set ZooKeeper symlink" && exit 1 )

    if ! zookeeperIsInstalled ; then
        fatal "ZooKeeper was not installed"
        exit 1
    else
        info "ZooKeeper tarball extracted and symlinked"
    fi
else
    info "ZooKeeper is already installed"
fi

# If Accumulo is not installed, bootstrap and verify that the two checksums match before installing.
if ! accumuloIsInstalled ; then
    bootstrapAccumulo
    verifyChecksum "${DW_ACCUMULO_DIST_URI}" "${DW_ACCUMULO_SERVICE_DIR}" "${DW_ACCUMULO_DIST_SHA512_CHECKSUM}" || exit 1

    info "Installing Accumulo..."
    [ -f "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_DIST}" ] || ( fatal "Accumulo tarball not found" && exit 1 )
    mkdir "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" || ( fatal "Failed to create Accumulo base directory" && exit 1 )
    # Extract Accumulo, set symlink, and verify...
    tar xf "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_DIST}" -C "${DW_ACCUMULO_SERVICE_DIR}/${DW_ACCUMULO_BASEDIR}" --strip-components=1 || ( fatal "Failed to extract Accumulo tarball" && exit 1 )
    ( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/accumulo/${DW_ACCUMULO_BASEDIR}" "${DW_ACCUMULO_SYMLINK}" ) || ( fatal "Failed to set Accumulo symlink" && exit 1 )

    if ! accumuloIsInstalled ; then
        fatal "Accumulo was not installed"
        exit 1
    else
        info "Accumulo tarball extracted and symlinked"
    fi
else
    info "Accumulo is already installed"
fi

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

assertCreateDir "${DW_ACCUMULO_JVM_HEAPDUMP_DIR}" || exit 1

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
   echo "${DW_ZOOKEEPER_CONF}" > "${DW_ZOOKEEPER_CONF_DIR}/zoo.cfg" || ( fatal "Failed to write zoo.cfg" && exit 1 )
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
   "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p "${DW_ACCUMULO_VFS_DATAWAVE_DIR}" || ( fatal "Failed to create ${DW_ACCUMULO_VFS_DATAWAVE_DIR}" && exit 1 )
fi

# Initialize Accumulo
"${ACCUMULO_HOME}/bin/accumulo" init \
 --clear-instance-name \
 --instance-name "${DW_ACCUMULO_INSTANCE_NAME}" \
 --password "${DW_ACCUMULO_PASSWORD}" || ( fatal "Failed to initialize Accumulo" && exit 1 )

echo
info "Accumulo initialized and ready to start..."
echo
echo "      Start command: accumuloStart"
echo "       Stop command: accumuloStop"
echo "     Status command: accumuloStatus"
echo
info "See \$DW_CLOUD_HOME/bin/services/accumulo/bootstrap.sh to view/edit commands as needed"
echo
