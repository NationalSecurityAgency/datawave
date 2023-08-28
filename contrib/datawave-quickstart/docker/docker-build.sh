#!/usr/bin/env bash

# This script builds the 'datawave-quickstart' Docker image and tags it with the user-specified arg.

# The context for the build is the entire DataWave source tree, DATAWAVE_SOURCE_DIR, including any pre-built /
# pre-downloaded binaries that exist under DATAWAVE_SOURCE_DIR/contrib/datawave-quickstart/

# See .dockerignore for exclusions and exceptions

function help() {
    echo "  $( printGreen "Usage:" )"
    echo "  ./$( basename ${BASH_SOURCE[0]} ) $( printGreen "<image tag name>" ) [[$( printGreen "--use-existing-binaries,-ueb" )] [$( printGreen "--docker-opts,-do" )] [$( printGreen "--help,-h")]]"
    echo
    echo "  $( printGreen "Examples:" )"
    echo "   ./$( basename ${BASH_SOURCE[0]} ) latest"
    echo "   ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT --docker-opts \"--squash --force-rm\""
    echo "   ./$( basename ${BASH_SOURCE[0]} ) 1.1.0-SNAPSHOT --use-existing-binaries"
    echo
    echo "  $( printGreen "Notes:" )"
    echo
    echo "   By default, this script will force a fresh build of DataWave's ingest and web"
    echo "   tarballs prior to the Docker build. This is to ensure that any path-specific"
    echo "   settings within DataWave are properly set prior to deployment to the image's "
    echo "   $( printGreen "/opt/datawave/contrib/datawave-quickstart" ) directory. Also, this avoids"
    echo "   building DataWave inside the image itself, which would incur the cost of"
    echo "   creating a large ~/.m2/repository inside the image as well"
    echo
    echo "   This strategy allows us to leverage the existing quickstart scripts to perform"
    echo "   most of the work here, as if for a typical deployment, except that we first set"
    echo "   $( printGreen "DW_ROOT_DIRECTORY_OVERRIDE" )=$( printGreen "/opt/datawave" ) in the shell environment, so that the"
    echo "   quickstart will inject that value into <maven-profile>.properties, which is"
    echo "   used, in turn, to generate DataWave's runtime configs during the build"
    echo
    echo "   Moreover, the $( printGreen "--use-existing-binaries" ) flag *can* be used as a time saver to skip"
    echo "   the DataWave build, if both tarballs already exist. However, you may safely"
    echo "   assume that your previously-built tarballs are valid ONLY under one (or both)"
    echo "   of the following circumstances:"
    echo
    echo "   - This script was executed previously and the DataWave-build phase of the script"
    echo "     had succeeded. For example, perhaps only the Docker-build phase had failed,"
    echo "     which you now have resolved and you are ready to try again"
    echo
    echo "   - Your current $( printGreen "DATAWAVE_SOURCE_DIR" ) happens to be $( printGreen "/opt/datawave" ) already, in which"
    echo "     case any existing binaries from a previous quickstart build will still be"
    echo "     properly configured by default"
    echo
    echo "     $( printGreen "Hint:" ) your current $( printGreen "DATAWAVE_SOURCE_DIR" ) is $( printGreen "${DATAWAVE_SOURCE_DIR}" )"
    echo
}

# DATAWAVE_SOURCE_DIR will be used as the build context

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
QUICKSTART_DIR="$( dirname "${THIS_DIR}" )"
DATAWAVE_SOURCE_DIR="$( cd ${QUICKSTART_DIR}/../../ && pwd )"

DW_DATAWAVE_BUILD_PROFILE=${DW_DATAWAVE_BUILD_PROFILE:-dev}

source "${QUICKSTART_DIR}/bin/logging.sh"

function validateArgs() {
   # First arg must be -h, --help, or <image tag name>

   [ -z "$1" ] && error "Tag name is required" && help && exit 1
   [[ "$1" == "-h" || "$1" == "--help" ]] && help && exit 0
   [[ "$1" =~ ^- ]] && error "'$1' does not appear to be a valid tag name" && help && exit 1

   readonly IMAGE_NAME="datawave/quickstart:$1"
   shift

   # Optional params

   docker_opts=""
   use_existing_binaries=false

   while [ -n "${1}" ]; do
       case "${1}" in
          --use-existing-binaries | -ueb)
             use_existing_binaries=true
             ;;
          --docker-opts | -do)
             docker_opts="${2}"
             shift
             ;;
          --help | -h)
             help && exit 0
             ;;
          *)
             fatal "Invalid argument passed to $( basename "$0" ): ${1}"
       esac
       shift
    done
}

function removeDatawaveTarball() {
    local tarballPath="$( find "${QUICKSTART_DIR}/bin/services/datawave" -path "${1}" -type f )"
    [ -n "${tarballPath}" ] && rm -f "${tarballPath}" && info "Removed binary: ${tarballPath}"

}

function cleanBuildContext() {
    info "Cleaning up"
    rm -f ${DATAWAVE_SOURCE_DIR}/.dockerignore > /dev/null 2>&1 && info ".dockerignore copy removed"

    # Remove any potentially stale DataWave tarballs from the DW service directory. Removal
    # will guarantee that a fresh Maven build is triggered whenever env.sh is sourced

    if [[ "${use_existing_binaries}" != true ]] ; then
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
    export DW_JAVA_HOME_OVERRIDE=/usr/lib/jvm/java-1.8.0-openjdk
}

function prepareBuildContext() {

    info "Preparing context for ${IMAGE_NAME}"

    cleanBuildContext

    overrideBuildProperties

    # Source env.sh to force all tarballs to be downloaded and the DW binaries to be built, if necessary

    source "${QUICKSTART_DIR}/bin/env.sh"

    datawaveBuildSucceeded || fatal "Most recent DataWave build failed. Cannot proceed with Docker build!"

    # Temporarily copy .dockerignore to DATAWAVE_SOURCE_DIR (i.e., root context for docker build)...

    cp "${THIS_DIR}/.dockerignore" "${DATAWAVE_SOURCE_DIR}/.dockerignore" || fatal "Failed to copy .dockerignore into place"

    sanityCheckTarball "${QUICKSTART_DIR}/bin/services/maven" "apache-maven*.tar.gz" "${DW_MAVEN_DIST_URI}"
}

function sanityCheckTarball() {
    # This is intended to verify that certain tarballs are staged and ready to be copied into
    # the image context as required. E.g., the maven bootstrap script *may* have favored a
    # preexisting local install, regardless of the configured 'DW_*_DIST_URI' variable's advice

    local serviceDir="${1}"
    local tarballPattern="${2}"
    local tarballDistUri="${3}"

    local serviceName="$( basename ${serviceDir} )"
    info "Performing sanity check for ${serviceName} tarball"

    local serviceTarball="$(find "${serviceDir}" -maxdepth 1 -type f -name "${tarballPattern}" | head -n 1)"
    if [ -z "${serviceTarball}" ] ; then
        # Remote tarball? Try to download it
        if [[ "${tarballDistUri}" =~ ^http ]] ; then
            info "Downloading $(basename "${tarballDistUri}")"
            $( cd "${serviceDir}" && wget ${DW_WGET_OPTS} "${tarballDistUri}" ) || fatal "Sanity check failed for http uri: ${tarballDistUri}"
        else
            # Assuming local tarball. Try to copy into place (after stripping 'file://' prefix)
            local localFilePath="${tarballDistUri:7}"
            if [ -f "${localFilePath}" ] ; then
                info "Copying local $(basename "${tarballDistUri}") into place"
                cp "${localFilePath}" "${serviceDir}/" || fatal "Sanity check failed for file uri: ${tarballDistUri}"
            else
                fatal "Sanity check failed. File DNE: ${localFilePath}"
            fi
        fi
        # Last try...
        serviceTarball="$(find "${serviceDir}" -maxdepth 1 -type f -name "${tarballPattern}" | head -n 1)"
        [ -z "${serviceTarball}" ] && \
           error "Sanity check failed. There is no tarball matching bin/services/${serviceName}/${tarballPattern}" && \
           error "Please override 'DW_${serviceName^^}_DIST_URI' variable to reference a valid local or remote tarball" && \
           fatal "Correct this issue and try again"
    fi
    info "Sanity check passed for $(basename "${serviceTarball}")"
}

function buildDockerImage() {

    info "Building Docker image: ${IMAGE_NAME}"

    docker build ${docker_opts} -f ${THIS_DIR}/Dockerfile -t ${IMAGE_NAME} \
         --build-arg DATAWAVE_COMMIT_ID=$( git rev-parse --verify HEAD ) \
         --build-arg DATAWAVE_BRANCH_NAME=$( git rev-parse --abbrev-ref HEAD ) \
         --build-arg DATAWAVE_JAVA_HOME="${DW_JAVA_HOME_OVERRIDE}" \
         ${DATAWAVE_SOURCE_DIR} || fatal "Docker image creation for DataWave Quickstart failed"
}

validateArgs "$@"

prepareBuildContext

buildDockerImage

cleanBuildContext

exit 0
