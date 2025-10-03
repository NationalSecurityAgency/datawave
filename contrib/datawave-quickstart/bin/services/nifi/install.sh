#!/usr/bin/env bash
set -e

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"

# if JDK is not installed exit early
jdkIsConfigured

# it might not be installed and we do not want to fail so we check with a traditional if statement
if nifiIsInstalled ; then
    info "NiFi is already installed"
    exit 0
fi

# bootstrap and verify that the two checksums match before installing.
bootstrapNifi
verifyChecksum "${DW_NIFI_DIST_URI}" "${DW_NIFI_SERVICE_DIR}" "${DW_NIFI_DIST_SHA512_CHECKSUM}" || exit 1

info "Installing NiFi..."
[ -f "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_DIST}" ] || ( fatal "NiFi zip file not found" && exit 1 )

# Extract, set symlink, and verify...
mkdir "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}" || ( fatal "Failed to create NiFi base directory" && exit 1 )
unzip "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_DIST}" -d "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}" || ( fatal "Failed to extract NiFi tarball" && exit 1 )
( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/nifi/${DW_NIFI_BASEDIR}" "${DW_NIFI_SYMLINK}" ) || ( fatal "Failed to create NiFi symlink" && exit 1 )
nifiIsInstalled || ( fatal "NiFi was not installed" && exit 1 )
info "NiFi installed"

echo
info "NiFi initialized and ready to start..."
echo
echo "      Start command: nifiStart"
echo "       Stop command: nifiStop"
echo "     Status command: nifiStatus"
echo
info "See \$DW_CLOUD_HOME/bin/services/nifi/bootstrap.sh to view/edit commands as needed"
echo
