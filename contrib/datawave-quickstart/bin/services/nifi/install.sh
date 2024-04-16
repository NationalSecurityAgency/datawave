#!/usr/bin/env bash

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"

# If NiFi is not installed, verify that the two checksums match before installing.
nifiIsInstalled || verifyChecksum "${DW_NIFI_DIST_URI}" "${DW_NIFI_SERVICE_DIR}" "${DW_NIFI_DIST_SHA512_CHECKSUM}"

nifiIsInstalled && info "NiFi is already installed" && exit 1

[ ! -f "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_DIST}" ] && fatal "NiFi tarball not found"

mkdir "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}" || fatal "Failed to create NiFi base directory"

tar xf "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_DIST}" -C "${DW_NIFI_SERVICE_DIR}/${DW_NIFI_BASEDIR}" --strip-components=1 || fatal "Failed to extract NiFi tarball"
$( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/nifi/${DW_NIFI_BASEDIR}" "${DW_NIFI_SYMLINK}" ) || fatal "Failed to create NiFi symlink"

nifiIsInstalled || fatal "NiFi was not installed"

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
