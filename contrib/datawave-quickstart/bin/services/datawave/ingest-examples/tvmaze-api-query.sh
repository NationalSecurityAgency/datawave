#!/usr/bin/env bash

#
# Performs api.tvmaze.com query for a specified show, and prints json response to stdout.
#
# Usage: ./tvmaze-api-query.sh <tv-show-name> [ -p ]
#
#         tv-show-name: required
#         -p: optional, pretty-prints formatted json
#

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname $( dirname "${THIS_DIR}" ) )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/logging.sh"
source "${BIN_DIR}/query.sh" # for urlencode function

CURL="$( which curl )"

TVMAZE_SHOWNAME="${1}"
[ -z "${TVMAZE_SHOWNAME}" ] && fatal "TV show argument is required!"

PRETTY=false
[ "${2}" == "-p" ] && PRETTY=true

TVMAZE_QUERY="http://api.tvmaze.com/singlesearch/shows?q=$(urlencode "${TVMAZE_SHOWNAME}")\&embed=cast"

CURL_CMD="${CURL} --silent --write-out 'HTTP_STATUS_CODE:%{http_code}' -X GET ${TVMAZE_QUERY}"
CURL_RESPONSE="$( eval "${CURL_CMD}" )"
CURL_EXIT=$?

[ "${CURL_EXIT}" != "0" ] && fatal "Curl command exited with non-zero status: ${CURL_EXIT}"

TVMAZE_RESPONSE_BODY=$( echo ${CURL_RESPONSE} | sed -e 's/HTTP_STATUS_CODE\:.*//g' )
TVMAZE_RESPONSE_STATUS=$( echo ${CURL_RESPONSE} | tr -d '\n' | sed -e 's/.*HTTP_STATUS_CODE://' )

[ "${TVMAZE_RESPONSE_STATUS}" != "200" ] && error "api.tvmaze.com returned invalid response status: ${TVMAZE_RESPONSE_STATUS}" && exit 1
[ -z "${TVMAZE_RESPONSE_BODY}" ] && error "Response body is empty!" && exit 1

PY_CMD='from __future__ import print_function; import sys,json; data=json.loads(sys.stdin.read()); print(json.dumps(data, indent=2, sort_keys=True))'
if [ "${PRETTY}" == true ] ; then
  echo "${TVMAZE_RESPONSE_BODY}" | ( python3 -c "${PY_CMD}" 2>/dev/null || python2 -c "${PY_CMD}" 2>/dev/null ) || ( warn "Unable to pretty print, Python not detected" && echo "${TVMAZE_RESPONSE_BODY}" )
else
  echo "${TVMAZE_RESPONSE_BODY}"
fi

exit 0