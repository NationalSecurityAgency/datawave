#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
QUICKSTART_DIR="$( dirname "${THIS_DIR}" )"

source "${QUICKSTART_DIR}/bin/logging.sh"

function usage() {
    echo "  $( printGreen "Usage:" ) $( basename ${BASH_SOURCE[0]} ) $( printGreen "<image tag name>" ) $( printGreen "< -it|-d >" ) [ $( printGreen "<OPTIONAL-COMMAND>" ) ]"
    echo "     E.g., ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT -it"
    echo "     E.g., ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT -it /bin/bash"
    echo "     E.g., ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT -it datawave-bootstrap.sh --ingest --bash"
    echo "     E.g., ./$( basename ${BASH_SOURCE[0]} ) 1.0.0-SNAPSHOT -d datawave-bootstrap.sh --ingest --web"
}

[[ -z "$1" || "$1" == "--help" || "$1" == "-h" ]] && usage && exit 0

[ -z "$2" ] && usage && exit 1

[[ "$2" != "-it" && "$2" != "-d" ]] && echo "Second argument must be -it or -d" && usage && exit 1

IMAGE_NAME="datawave-quickstart:$1"

# We're creating a named, external Docker volume on the fly for the container's primary data
# directory (ie, /opt/datawave/contrib/datawave-quickstart/data), which will allow the user to
# ingest more data, if desired, to avoid all the pitfalls involved with writing to the internal
# image layer in the container.

# Creating the volume in this way also ensures that it gets populated with the base image's
# existing contents on startup, which is not the case for Docker bind mounts

DATA="datawave-vol-$(date +%Y-%m-%d-%H%M%S)" && V_DATA="/opt/datawave/contrib/datawave-quickstart/data"

# Set up simple bind mounts in /tmp/ for log files and other cruft

HLOGS=/tmp/dwlogs/hadoop && V_HLOGS=/opt/datawave/contrib/datawave-quickstart/hadoop/logs && [ ! -d ${HLOGS} ] && mkdir -p ${HLOGS}
ALOGS=/tmp/dwlogs/accumulo && V_ALOGS=/opt/datawave/contrib/datawave-quickstart/accumulo/logs && [ ! -d ${ALOGS} ] && mkdir -p ${ALOGS}
WLOGS=/tmp/dwlogs/wildfly && V_WLOGS=/opt/datawave/contrib/datawave-quickstart/wildfly/standalone/log && [ ! -d ${WLOGS} ] && mkdir -p ${WLOGS}

# Bind mount for local Maven repo, in case we decide to rebuild DataWave

M2REPO=~/.m2/repository && V_M2REPO=/root/.m2/repository && [ ! -d ${M2REPO} ] && mkdir -p ${M2REPO}

# Set volume mapping

VOLUMES="-v ${DATA}:${V_DATA} -v ${M2REPO}:${V_M2REPO} -v ${HLOGS}:${V_HLOGS} -v ${ALOGS}:${V_ALOGS} -v ${WLOGS}:${V_WLOGS}"

# Set port mapping

PORTS="-p 8443:8443 -p 50070:50070 -p 9995:9995"

# Interpret any remaining args as the CMD to pass in

COMMAND="${@:3}"

DOCKER_RUN="docker run $2 --rm ${VOLUMES} ${PORTS} --memory-swappiness=0 ${IMAGE_NAME} ${COMMAND}"

eval "${DOCKER_RUN}"
