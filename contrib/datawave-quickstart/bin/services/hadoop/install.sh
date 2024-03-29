#!/usr/bin/env bash

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

function verifySSHConfig() {
   # Create ssh key for password-less ssh, if necessary
   if [ ! -f ~/.ssh/id_rsa ] ; then
      if askYesNo "Hadoop needs password-less ssh, but no ssh key (~/.ssh/id_rsa) was found
Generate the password-less ssh key now?"
      then
         echo "Generating password-less ssh key: ~/.ssh/id_rsa" && sleep 3
         ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa || fatal "SSH config failed! Please configure manually"
         echo "Adding ssh key to ~/.ssh/authorized_keys file"
         cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
         chmod 0600 ~/.ssh/authorized_keys
         ssh-agent -s
         echo "Adding key to ssh-agent"
         if ! ssh-add; then
             eval "$(ssh-agent)" && ssh-add || warn "Error adding ssh key. SSH issues may cause Hadoop installation to fail"
         fi
      fi
   elif [ ! -f ~/.ssh/authorized_keys ] ; then
      if [ -f ~/.ssh/id_rsa.pub ] ; then
         echo "Adding existing ssh key to ~/.ssh/authorized_keys file"
         cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
         chmod 0600 ~/.ssh/authorized_keys
      else
         error "Hadoop needs password-less ssh, but neither authorized_keys nor id_rsa.pub was found. Please configure ~/.ssh appropriately"
      fi
   fi
}

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"

verifyChecksum "${DW_HADOOP_DIST_URI}" "${DW_HADOOP_SERVICE_DIR}" "${DW_HADOOP_DIST_SHA512_CHECKSUM}"

hadoopIsInstalled && info "Hadoop is already installed" && exit 1

[ -f "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_DIST}" ] || fatal "Hadoop tarball not found"

sshIsInstalled || fatal "It appears that ssh and/or sshd are not installed. Please correct this before continuing"

# Extract, set symlink, and verify...
mkdir "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_BASEDIR}" || fatal "Failed to create Hadoop base directory"
tar xf "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_DIST}" -C "${DW_HADOOP_SERVICE_DIR}/${DW_HADOOP_BASEDIR}" --strip-components=1 || fatal "Failed to extract Hadoop tarball"
( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/hadoop/${DW_HADOOP_BASEDIR}" "${DW_HADOOP_SYMLINK}" ) || fatal "Failed to set Hadoop symlink"

! hadoopIsInstalled && fatal "Hadoop was not installed"

info "Hadoop tarball extracted and symlinked"

# Write *-site.xml files...

if [ ! -z "${DW_HADOOP_CORE_SITE_CONF}" ] ; then
   writeSiteXml "${HADOOP_CONF_DIR}/core-site.xml" "${DW_HADOOP_CORE_SITE_CONF}" || fatal "Failed to write core-site.xml"
   info "Hadoop core-site.xml written"
else
   warn "No core-site.xml content defined! :("
fi

if [ ! -z "${DW_HADOOP_HDFS_SITE_CONF}" ] ; then
   writeSiteXml "${HADOOP_CONF_DIR}/hdfs-site.xml" "${DW_HADOOP_HDFS_SITE_CONF}" || fatal "Failed to write hdfs-site.xml"
   info "Hadoop hdfs-site.xml written"
else
   warn "No hdfs-site.xml content defined! :("
fi

if [ ! -z "${DW_HADOOP_MAPRED_SITE_CONF}" ] ; then
   writeSiteXml "${HADOOP_CONF_DIR}/mapred-site.xml" "${DW_HADOOP_MAPRED_SITE_CONF}" || fatal "Failed to write mapred-site.xml"
   info "Hadoop mapred-site.xml written"
else
   warn "No mapred-site.xml content defined! :("
fi

if [ ! -z "${DW_HADOOP_YARN_SITE_CONF}" ] ; then
   writeSiteXml "${HADOOP_CONF_DIR}/yarn-site.xml" "${DW_HADOOP_YARN_SITE_CONF}" || fatal "Failed to write yarn-site.xml"
   info "Hadoop yarn-site.xml written"
else
   warn "No yarn-site.xml content defined! :("
fi

if [ ! -z "${DW_HADOOP_CAPACITY_SCHEDULER_CONF}" ] ; then
   writeSiteXml "${HADOOP_CONF_DIR}/capacity-scheduler.xml" "${DW_HADOOP_CAPACITY_SCHEDULER_CONF}" || fatal "Failed to write capacity-scheduler.xml"
   info "Hadoop capacity-scheduler.xml written"
else
   warn "No capacity-scheduler.xml content defined! :("
fi

# Ensure that $JAVA_HOME is observed by all hadoop scripts
sed -i'' -e "s|.*\(export JAVA_HOME=\).*|\1${JAVA_HOME}|g" ${HADOOP_CONF_DIR}/hadoop-env.sh

verifySSHConfig

# Format namenode
${HADOOP_HOME}/bin/hdfs namenode -format || fatal "Failed to initialize Hadoop"

echo
info "Hadoop initialized and ready to start..."
echo
echo "      Start command: hadoopStart"
echo "       Stop command: hadoopStop"
echo "     Status command: hadoopStatus"
echo
info "See \$DW_CLOUD_HOME/bin/services/hadoop/bootstrap.sh to view/edit commands as needed"
echo
