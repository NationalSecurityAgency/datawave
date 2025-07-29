#!/usr/bin/env bash
set -e

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"
source "${SERVICES_DIR}/hadoop/bootstrap.sh"
source "${SERVICES_DIR}/accumulo/bootstrap.sh"

# if JDK is not installed exit early
jdkIsConfigured

accumuloIsInstalled || ( fatal "DataWave Web requires that Accumulo be installed" && exit 1 )

# it might not be installed and we do not want to fail so we check with a traditional if statement
if datawaveWebIsInstalled ; then
    info "DataWave Web is already installed"
    exit 0
fi

# If Wildfly is not installed, bootstrap and verify that the two checksums match before installing.
if wildflyIsInstalled; then
    info "Wildfly is already installed"
else
    bootstrapWildfly
    verifyChecksum "${DW_WILDFLY_DIST_URI}" "${DW_DATAWAVE_SERVICE_DIR}" "${DW_WILDFLY_DIST_SHA512_CHECKSUM}" || exit 1

    info "Installing Wildfly..."
    TARBALL_BASE_DIR="${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_BASEDIR}"

    mkdir "${TARBALL_BASE_DIR}" || ( fatal "Failed to create Wildfly base directory" && exit 1 )
    tar xf "${DW_DATAWAVE_SERVICE_DIR}/${DW_WILDFLY_DIST}" -C "${TARBALL_BASE_DIR}" --strip-components=1 || ( fatal "Failed to extract Wildfly tarball" && exit 1 )
    ( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/datawave/${DW_WILDFLY_BASEDIR}" "${DW_WILDFLY_SYMLINK}" ) || ( fatal "Failed to set Wildfly symlink" && exit 1 )
fi

info "Installing DataWave Web..."
getDataWaveTarball "${DW_DATAWAVE_WEB_TARBALL}"
DW_DATAWAVE_WEB_DIST="${tarball}"
[ -f "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_DIST}" ] || ( fatal "DataWave Web tarball not found" && exit 1 )

TARBALL_BASE_DIR="${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_BASEDIR}"

# Extract, set symlink, and verify...
mkdir "${TARBALL_BASE_DIR}" || ( fatal "Failed to create DataWave Web base directory" && exit 1 )
tar xf "${DW_DATAWAVE_SERVICE_DIR}/${DW_DATAWAVE_WEB_DIST}" -C "${TARBALL_BASE_DIR}" --strip-components=1 || ( fatal "Failed to extract DataWave Web tarball" && exit 1 )
( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/datawave/${DW_DATAWAVE_WEB_BASEDIR}" "${DW_DATAWAVE_WEB_SYMLINK}" ) || ( fatal "Failed to set DataWave Web symlink" && exit 1 )
datawaveWebIsInstalled || ( fatal "DataWave Web was not installed" && exit 1 )
info "DataWave Web tarballs extracted and symlinked"

info "Configuring Wildfly for DataWave..."
( cd "${DW_CLOUD_HOME}/${DW_DATAWAVE_WEB_SYMLINK}" && ./setup-wildfly.sh )

# Set JVM properties on Wildfly as needed
mkdir ${TARBALL_BASE_DIR}/bin
cat << EOF >> ${TARBALL_BASE_DIR}/bin/standalone.conf
JAVA_OPTS="\$JAVA_OPTS -Daccumulo.properties=file://${ACCUMULO_HOME}/conf/accumulo.properties"
EOF

echo
info "DataWave Web initialized and ready to start..."
echo
echo "       Start command: datawaveWebStart [-d|--debug]"
echo "        Stop command: datawaveWebStop"
echo "      Status command: datawaveWebStatus"
echo "        Test command: datawaveWebTest [-h|--help] ..."
echo "       Build command: datawaveBuild"
echo "    Redeploy command: datawaveBuildDeploy"
echo "       Query command: datawaveQuery [-h|--help] ..."
echo
info "See \$DW_CLOUD_HOME/bin/services/datawave/bootstrap-web.sh to view/edit commands as needed"

#askYesNo "Would you like to start up DataWave web services?" && datawaveWebStart
