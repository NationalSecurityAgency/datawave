#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname $( dirname "${THIS_DIR}" ) )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

# Import some helpful bits from the quickstart environment...

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/../bootstrap.sh"

# Default list of shows to ingest. May be overridden with the --shows,-s parameter

TV_SHOW_LIST="Veep, Game of Thrones, I Love Lucy, Breaking Bad, Malcom in the Middle, The Simpsons, Sneaky Pete, King of the Hill, Three's Company, The Andy Griffith Show, Matlock, North and South, MASH"

# Default staging file for the raw data

OUTPUT_STAGING_FILE="${DW_DATAWAVE_DATA_DIR}/tv-show-raw-data-$(date +%Y%m%d%H%M%S).json"

# Toggle download-only mode

DOWNLOAD_ONLY=true

function usage() {
   echo
   echo " This script downloads and ingests TV show datasets in JSON format from api.tvmaze.com..."
   echo
   echo "     $( printGreen $( basename "$0" )) can be invoked with no arguments."
   echo
   echo " Default list of shows to download and ingest: "
   echo
   echo "     ${TV_SHOW_LIST}"
   echo
   echo " $( printGreen "-s" ) | $( printGreen "--shows" ) \"TV Show 1, TV Show 2 , TV Show 3 ,..., TV Show N\""
   echo
   echo "      Comma-delimited list of TV shows to download and ingest. Overrides the default list. Show names will be"
   echo "      automatically trimmed and URL encoded for proper handling of spaces & other special characters. If a show"
   echo "      name includes one or more commas, it will need to be URL encoded prior to passing into this script"
   echo
   echo " $( printGreen "-o" ) | $( printGreen "--outfile" ) /path/to/local/filename"
   echo
   echo "      Override the default local output file for staging the raw TV show data"
   echo "      Default: \${DW_DATAWAVE_DATA_DIR}/tv-show-raw-data-\$(date +%Y%m%d%H%M%S).json"
   echo
   echo " $( printGreen "-d" ) | $( printGreen "--download-only" )"
   echo
   echo "      Disable ingest. Only download data and write it to file"
   echo
   echo " $( printGreen "-h" ) | $( printGreen "--help" )"
   echo
   echo "      Print this usage information and exit"
   echo

   exit 0
}

function configure() {

   while [ "${1}" != "" ]; do
      case "${1}" in
         --shows | -s)
            TV_SHOW_LIST="${2}"
            shift
            ;;
         --outfile | -o)
            OUTPUT_STAGING_FILE="${2}"
            shift
            ;;
         --download-only | -d)
            DOWNLOAD_ONLY=true
            ;;
         --help | -h)
            usage
            ;;
         *)
            error "Invalid argument passed to $( basename "$0" ): ${1}" && exit 1
      esac
      shift
   done

   [ -z "${TV_SHOW_LIST}" ] && error "TV show list can not be empty" && exit 1
   [ -z "${OUTPUT_STAGING_FILE}" ] && error "Output staging file path can not be empty" && exit 1

   touch "${OUTPUT_STAGING_FILE}" || fatal "Can't access ${OUTPUT_STAGING_FILE}"
}

function trim() {
    local var="${1}"

    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"

    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"

    echo "$var"
}
configure "$@"

OLD_IFS="$IFS"
IFS=","
SHOWS_ARRAY=( ${TV_SHOW_LIST} )
IFS="$OLD_IFS"

info "Writing json records to ${OUTPUT_STAGING_FILE}"
for SHOW_ID in {30001..90000}; do 
    echo "retrieving $SHOW_ID"
    ${THIS_DIR}/tvmaze-id-query.sh ${SHOW_ID} -p >> "${OUTPUT_STAGING_FILE}" 
#    curl --silent -X GET http://api.tvmaze.com/shows/${SHOW_ID}?embed=cast >> "${OUTPUT_STAGING_FILE}"
    sleep 1
#for SHOW in "${SHOWS_ARRAY[@]}" ; do
#    trimmed="$( trim "${SHOW}" )" info "Downloading show data: '${trimmed}'" # urlencode function defined in bin/query.sh...  ${THIS_DIR}/tvmaze-api-query.sh "${trimmed}" -p >> "${OUTPUT_STAGING_FILE}" done
done

info "Data download is complete"

exit 1
[ "${DOWNLOAD_ONLY}" == true ] && exit 0

[ ! -s "${OUTPUT_STAGING_FILE}" ] && info "${OUTPUT_STAGING_FILE} is empty. Nothing to ingest!" && exit 0

# See datawave/bootstrap.sh and accumulo/bootstrap.sh for these function definitions...
