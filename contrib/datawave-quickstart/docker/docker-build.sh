#!/usr/bin/env bash

# This script builds the 'datawave-quickstart' Docker image and tags it with the user-specified arg.

# The context for the build is the entire DataWave source tree, DATAWAVE_SOURCE_DIR, including any pre-built /
# pre-downloaded binaries that exist under DATAWAVE_SOURCE_DIR/contrib/datawave-quickstart/

# See .dockerignore for exclusions and exceptions

function help() {
    echo
    echo "  $( printGreen "Usage:" ) ./$( basename ${BASH_SOURCE[0]} ) $( printGreen "<image tag name>" ) [ $( printGreen "--use-existing-binaries" ) ]"
    echo
    echo "     E.g., ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT"
    echo "     E.g., ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT --use-existing-binaries"
    echo
    echo "     By default, this script will force a fresh build of DataWave's ingest and web tarballs to ensure"
    echo "     that they're properly configured for container deployment...that is, configured for deployment to"
    echo "     the Docker image's $( printGreen "/opt/datawave" ) directory"
    echo
    echo "     The optional $( printGreen "--use-existing-binaries" ) flag can be used as a time saver to skip the DataWave build if"
    echo "     both binaries already exist, but it should only be used under the following circumstances:"
    echo
    echo "     $( printGreen "1)" ) Your current $( printGreen "DATAWAVE_SOURCE_DIR" ) happens to be $( printGreen "/opt/datawave" ) already, in which case the"
    echo "        existing binaries from your datawave-quickstart deployment are properly configured by default."
    echo "        $( printGreen "Hint:" ) your current $( printGreen "DATAWAVE_SOURCE_DIR" ) = $( printGreen "${DATAWAVE_SOURCE_DIR}" )"
    echo "        OR..."
    echo
    echo "     $( printGreen "2)" ) This script has just run and the forced DataWave build succeeded, but the subsequent docker"
    echo "        build failed. Since this script sets $( printGreen "DW_ROOT_DIRECTORY_OVERRIDE" ) = $( printGreen "/opt/datawave" ) in the environment"
    echo "        prior to the build, you may assume that the resulting binaries are still valid"
    echo
}

# DATAWAVE_SOURCE_DIR will be used as the build context

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
QUICKSTART_DIR="$( dirname "${THIS_DIR}" )"
DATAWAVE_SOURCE_DIR="$( cd ${QUICKSTART_DIR}/../../ && pwd )"

DW_DATAWAVE_BUILD_PROFILE=${DW_DATAWAVE_BUILD_PROFILE:-dev}

source "${QUICKSTART_DIR}/bin/logging.sh"

function validateArgs() {
   [[ "$1" == "-h" || "$1" == "--help" ]] && help && exit 0
   [ -z "$1" ] && error "Tag name is required" && help && exit 1
   [ "$1" == "--use-existing-binaries" ] && error "First argument must be the tag name" && help && exit 1
   [ "$2" == "--use-existing-binaries" ] && USE_EXISTING_BINARIES=true

   IMAGE_NAME="datawave-quickstart:$1"
}

function removeDatawaveTarball() {
    local tarballPath="$( find "${QUICKSTART_DIR}/bin/services/datawave" -path "${1}" -type f )"
    [ -n "${tarballPath}" ] && rm -f "${tarballPath}" && info "Removed binary: ${tarballPath}"

}

function cleanBuildContext() {
    info "Cleaning up"
    unlink ${DATAWAVE_SOURCE_DIR}/.dockerignore > /dev/null 2>&1 && info ".dockerignore symlink removed"

    # Remove any potentially stale DataWave tarballs from the DW service directory. Removal
    # will guarantee that a fresh Maven build is triggered whenever env.sh is sourced

    if [[ "${USE_EXISTING_BINARIES}" != true ]] ; then
        removeDatawaveTarball "*/datawave-${DW_DATAWAVE_BUILD_PROFILE}-*-dist.tar.gz"
        removeDatawaveTarball "*/datawave-ws-deploy-application-*-${DW_DATAWAVE_BUILD_PROFILE}.tar.gz"
    else
        info "Retaining any existing DataWave binaries"
    fi
}

function overrideBuildProperties() {

    # Before we force a fresh DW build, set overrides for the current root directory and JAVA_HOME
    # within dev.properties, to ensure deployment is properly configured for the Docker image.

    # See services/datawave/bootstrap.sh, setBuildPropertyOverrides function

    export DW_ROOT_DIRECTORY_OVERRIDE=/opt/datawave
    export DW_JAVA_HOME_OVERRIDE=${DW_ROOT_DIRECTORY_OVERRIDE}/contrib/datawave-quickstart/java

}

function prepareBuildContext() {

    info "Preparing context for ${IMAGE_NAME}"

    cleanBuildContext

    overrideBuildProperties

    # Source env.sh to force all tarballs to be downloaded and the DW binaries to be built, if necessary

    source "${QUICKSTART_DIR}/bin/env.sh"

    datawaveBuildSucceeded || fatal "Most recent DataWave build failed. Cannot proceed with Docker build!"

    # Set temporary .dockerignore symlink in DATAWAVE_SOURCE_DIR for context exclusions...

    ln -s "${THIS_DIR}/.dockerignore" "${DATAWAVE_SOURCE_DIR}/.dockerignore" || fatal "Failed to set .dockerignore symlink"
}

function buildDockerImage() {

    info "Building Docker image: ${IMAGE_NAME}"

    docker build --squash --rm -f ${THIS_DIR}/Dockerfile -t ${IMAGE_NAME} \
         --build-arg DATAWAVE_COMMIT_ID=$( git rev-parse --verify HEAD ) \
         --build-arg DATAWAVE_BRANCH_NAME=$( git rev-parse --abbrev-ref HEAD ) \
         ${DATAWAVE_SOURCE_DIR} || fatal "Docker image creation for DataWave Quickstart failed"

}

validateArgs "$1" "$2"

prepareBuildContext

buildDockerImage

cleanBuildContext

exit 0
