
DW_BASE_URI=${DW_BASE_URI:-https://localhost:8443/DataWave}
DW_QUERY_URI=${DW_QUERY_URI:-${DW_BASE_URI}/Query}

function urlencode() {
   # Url encodes ${1}
   local LANG=C i c e=''
   for (( i=0; i < ${#1}; i++ )); do
       c=${1:$i:1}
       [[ "$c" =~ [a-zA-Z0-9\.\~\_\-] ]] || printf -v c '%%%02X' "'$c"
       e+="$c"
   done
   echo "$e"
}

function datawaveQuery() {

   ! datawaveIsInstalled && info "DataWave Web is not installed. Try 'datawaveInstall'" && return 1
   ! datawaveWebIsRunning && info "DataWave Web is not running. Try 'datawaveWebStart'" && return 1

   # Reset

   DW_QUERY=""
   DW_QUERY_RESPONSE_BODY=""
   DW_QUERY_RESPONSE_CODE=""
   DW_QUERY_RESPONSE_TYPE=""
   DW_QUERY_TOTAL_TIME=""
   DW_QUERY_EXTRA_PARAMS=""

   # Both 'Content-Type: application/x-www-form-urlencoded' and 'Accept: application/json'
   # added by default, but may be overridden, if needed, via --header,-H option

   DW_REQUEST_HEADERS=""

   # Defaults

   DW_QUERY_LOGIC="EventQuery"
   DW_QUERY_NAME="Query_$(date +%Y%m%d%H%M%S)"
   DW_QUERY_AUTHS="BAR,FOO,PRIVATE,PUBLIC"
   DW_QUERY_SYNTAX="LUCENE"
   DW_QUERY_BEGIN="19700101"
   DW_QUERY_END="20990101"
   DW_QUERY_LOG_VIZ="BAR&FOO"
   DW_QUERY_PAGESIZE=10

   # By default, creates query and gets first page of results w/ one request.
   # Use --create-only,-C flag to use "create" mode instead

   DW_QUERY_CREATE_MODE="createAndNext"

   DW_QUERY_VERBOSE=false

   configureUserIdentity || return 1
   configureQuery "$@" || return $?

   local curlcmd="/usr/bin/curl \
    --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
    --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" --keepalive-time 180 \
    --header 'Content-Type: application/x-www-form-urlencoded;charset=UTF-8' --header 'Accept: application/json' \
    ${DW_REQUEST_HEADERS} ${DW_CURL_DATA} -X POST ${DW_QUERY_URI}/${DW_QUERY_LOGIC}/${DW_QUERY_CREATE_MODE}"

   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?

   if [ "${exitStatus}" != "0" ] ; then
       echo
       error "Curl command exited with non-zero status: ${exitStatus}"
       echo
       return 1
   fi

   parseQueryResponse
   prettyPrintResponse
   setQueryIdFromResponse
   printCurlSummary

   return 0
}

function printCurlSummary() {

   if [ "${DW_QUERY_VERBOSE}" == true ] ; then
       echo "$( printGreen "Command" ): ${curlcmd}"
       echo
   fi
   echo "$( printGreen "Time" ): ${DW_QUERY_TOTAL_TIME} $( printGreen "Response Code" ): ${DW_QUERY_RESPONSE_CODE} $( printGreen "Response Type" ): ${DW_QUERY_RESPONSE_TYPE}"
   echo
   if [[ "${DW_QUERY_RESPONSE_CODE}" != 200 && "${DW_QUERY_RESPONSE_CODE}" != 204 ]] ; then
       warn "Response code '${DW_QUERY_RESPONSE_CODE}' indicates an error occurred. Check Wildfly logs, if needed"
       echo
   fi
}

function prettyPrintResponse() {
   echo
   if [ -n "${DW_QUERY_RESPONSE_BODY}" ] ; then

       if [ "${DW_QUERY_RESPONSE_TYPE}" == "application/json" ] ; then
           prettyPrintJson "${DW_QUERY_RESPONSE_BODY}"

       elif [ "${DW_QUERY_RESPONSE_TYPE}" == "application/xml" ] ; then
           prettyPrintXml "${DW_QUERY_RESPONSE_BODY}"

       else
           printRawResponse "${DW_QUERY_RESPONSE_BODY}"
       fi
   else
       if [ "${DW_QUERY_RESPONSE_CODE}" == 204 ] ; then
           info "No results for this query, as indicated by response code '204'"
           echo
       else
           info "No response body to print"
           echo
       fi
   fi
}

function setQueryIdFromResponse() {
   DW_QUERY_ID=""
   if [[ -n "${DW_QUERY_RESPONSE_TYPE}" && -n "${DW_QUERY_RESPONSE_BODY}" ]] ; then
       if [ "${DW_QUERY_RESPONSE_TYPE}" == "application/json" ] ; then
           setQueryIdFromResponseJson "${DW_QUERY_RESPONSE_BODY}"
       elif [ "${DW_QUERY_RESPONSE_TYPE}" == "application/xml" ] ; then
           setQueryIdFromResponseXml "${DW_QUERY_RESPONSE_BODY}"
       else
           warn "I don't know how to parse query id from this type: ${DW_QUERY_RESPONSE_TYPE}"
       fi
       echo "$( printGreen "Query ID" ): ${DW_QUERY_ID}"
       echo
   fi
}

function prettyPrintJson() {
    PY_CMD='from __future__ import print_function; import sys,json; data=json.loads(sys.stdin.read()); print(json.dumps(data, indent=2, sort_keys=True))'
    echo "${1}" | ( python3 -c "${PY_CMD}" 2>/dev/null || python2 -c "${PY_CMD}" 2>/dev/null ) || ( warn "Python encountered error. Printed response without formatting" && printRawResponse "${1}" )
}

function printRawResponse() {
    echo "${1}"
    echo
}

function prettyPrintXml() {
    local XMLLINT=$( which xmllint )
    if [ -n "${XMLLINT}" ] ; then
        echo "${1}" | ${XMLLINT} --format -
        local exitStatus=$?
        echo
        if [ "${exitStatus}" != "0" ] ; then
           printRawResponse "${1}"
           warn "xmllint encountered error. Printed response without formatting"
        fi
    else
        printRawResponse "${1}"
        warn "Couldn't find xmllint in your environment. Xml was printed without formatting"
        echo
    fi
}

function configureQuery() {

   while [ "${1}" != "" ]; do
      case "${1}" in
         --query | -q)
            DW_QUERY="$( urlencode "${2}" )"
            shift
            ;;
         --logic | -l)
            DW_QUERY_LOGIC="${2}"
            shift
            ;;
         --begin | -b)
            DW_QUERY_BEGIN="${2}"
            shift
            ;;
         --end | -e)
            DW_QUERY_END="${2}"
            shift
            ;;
         --log-visibility)
            DW_QUERY_LOG_VIZ="${2}"
            shift
            ;;
         --auths | -a)
            DW_QUERY_AUTHS="${2}"
            shift
            ;;
         --syntax | -s)
            DW_QUERY_SYNTAX="${2}"
            shift
            ;;
         --pagesize | -p)
            DW_QUERY_PAGESIZE="${2}"
            shift
            ;;
         --create-only | -C)
            DW_QUERY_CREATE_MODE="create"
            ;;
         --use-execute | -E)
            DW_QUERY_CREATE_MODE="execute"
            ;;
         --next | -n)
            # Get the next page and bail out
            DW_QUERY_ID="${2}"
            getNextPage
            return 2
            ;;
         --close | -c)
            # Close the query and bail out
            DW_QUERY_ID="${2}"
            closeQuery
            return 3
            ;;
         --param | -P)
            DW_QUERY_EXTRA_PARAMS="${DW_QUERY_EXTRA_PARAMS} ${2%%=*}=$( urlencode "${2#*=}" )"
            shift
            ;;
         --header | -H)
            DW_REQUEST_HEADERS="${DW_REQUEST_HEADERS} ${1} '${2}'"
            shift
            ;;
         --xml | -x)
            DW_REQUEST_HEADERS="${DW_REQUEST_HEADERS} --header 'Accept: application/xml'"
            ;;
         --help | -h)
            queryHelp && return 1
            ;;
         --verbose | -v)
            DW_QUERY_VERBOSE=true
            ;;
         *)
            error "Invalid argument passed to $( basename "$0" ): ${1}" && return 1
      esac
      shift
   done

   [ -z "${DW_QUERY}" ] && error "Query expression is required" && return 1

   setCurlData "query=${DW_QUERY}" \
               "queryName=${DW_QUERY_NAME}" \
               "auths=${DW_QUERY_AUTHS}" \
               "begin=${DW_QUERY_BEGIN}" \
               "end=${DW_QUERY_END}" \
               "pagesize=${DW_QUERY_PAGESIZE}" \
               "query.syntax=${DW_QUERY_SYNTAX}" \
               "columnVisibility=$( urlencode "${DW_QUERY_LOG_VIZ}" )" \
               ${DW_QUERY_EXTRA_PARAMS}
}

function queryHelp() {
    echo
    echo " The $( printGreen "datawaveQuery" ) shell function allows you submit queries on demand to DataWave's"
    echo " Rest API and to inspect query results. It automatically configures curl and sets"
    echo " reasonable defaults for most required query parameters"
    echo
    echo " E.g., $( printGreen datawaveQuery ) --query \"PAGE_TITLE:*Computing\""
    echo "       $( printGreen datawaveQuery ) --next 09aa3d46-8aa0-49fb-8859-f3add48859b0"
    echo "       $( printGreen datawaveQuery ) --close 09aa3d46-8aa0-49fb-8859-f3add48859b0"
    echo
    echo " Required:"
    echo
    echo " $( printGreen "-q" ) | $( printGreen "--query" ) \"<expression>\""
    echo "  Query expression. LUCENE syntax is assumed. Override via $( printGreen "--syntax" ),$( printGreen "-s" ) for JEXL, other"
    echo
    echo " Optional:"
    echo
    echo " $( printGreen "-l" ) | $( printGreen "--logic" ) \"<logicName>\""
    echo "  Specify the logic name to utilize for the query. Defaults to \"${DW_QUERY_LOGIC}\""
    echo
    echo " $( printGreen "-b" ) | $( printGreen "--begin" ) \"yyyyMMdd[ HHmmss.SSS]\""
    echo "  Begin date for query's shard date range. Defaults to ${DW_QUERY_BEGIN}"
    echo
    echo " $( printGreen "-e" ) | $( printGreen "--end" ) \"yyyyMMdd[ HHmmss.SSS]\""
    echo "  End date for query's shard date range. Defaults to ${DW_QUERY_END}"
    echo
    echo " $( printGreen "-a" ) | $( printGreen "--auths" ) \"A1,A2,A3,...\""
    echo "  List of Accumulo auths to enable for the query. Only data with these auths will be returned"
    echo "  Defaults to \"${DW_QUERY_AUTHS}\" to match those used on quickstart sample data"
    echo
    echo " $( printGreen "-s" ) | $( printGreen "--syntax" ) <syntax>"
    echo "  Identifies the query expression syntax being used. E.g., LUCENE, JEXL, etc. Defaults to ${DW_QUERY_SYNTAX}"
    echo
    echo " $( printGreen "-p" ) | $( printGreen "--pagesize" ) <int>"
    echo "  Sets the page size to be used for each page of query results. Defaults to ${DW_QUERY_PAGESIZE}"
    echo
    echo " $( printGreen "--log-visibility" ) <visibility-expression>"
    echo "  Visibility expression to use when logging this query to Accumulo. Defaults to '${DW_QUERY_LOG_VIZ}'"
    echo
    echo " $( printGreen "-C" ) | $( printGreen "--create-only" )"
    echo "  Uses the 'Query/{logic}/create' endpoint, rather than the default, 'Query/{logic}/createAndNext' which creates query and gets first page w/ one request"
    echo
    echo " $( printGreen "-n" ) | $( printGreen "--next" ) <query-id> "
    echo "  Gets the next page of results for the specified query id. Response code 204 indicates end of result set"
    echo
    echo " $( printGreen "-c" ) | $( printGreen "--close" ) <query-id> "
    echo "  Releases all server side resources being utilized for the specified query id"
    echo
    echo " $( printGreen "-P" ) | $( printGreen "--param" ) \"paramName=paramValue\""
    echo "  Adds the specified query parameter name/value. If needed, use \"paramName=\$( $(printGreen "urlencode") 'paramValue' )\""
    echo
    echo " $( printGreen "-H" ) | $( printGreen "--header" ) \"HeaderName: HeaderValue\""
    echo "  Adds specified name/value pair to the curl command as an HTTP request header"
    echo "  Defaults: '$(printGreen "Content-Type"): application/x-www-form-urlencoded' and '$(printGreen "Accept"): application/json'"
    echo
    echo " $( printGreen "-x" ) | $( printGreen "--xml" )"
    echo "  Adds '$(printGreen "Accept"): application/xml' as an HTTP request header to override the default JSON"
    echo
    echo " $( printGreen "-v" ) | $( printGreen "--verbose" )"
    echo "  Display curl command. Otherwise, only query results and response metadata are displayed"
    echo
    echo " $( printGreen "-h" ) | $( printGreen "--help" )"
    echo "  Print this usage information and exit the script"
    echo
}

function closeQuery() {

   [ -z "${DW_QUERY_ID}" ] && error "DW_QUERY_ID is null. Nothing to close" && return 1
   [ -z "$( echo ${DW_QUERY_ID} | grep -E '^[a-zA-Z0-9\-]+$' )" ] && error "'${DW_QUERY_ID}' is not a valid query id" && return 1

   local curlcmd="/usr/bin/curl \
   --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
   --insecure --cert ${DW_CURL_CERT} --key ${DW_CURL_KEY_RSA} --cacert ${DW_CURL_CA} --keepalive-time 180 \
   -X PUT ${DW_QUERY_URI}/${DW_QUERY_ID}/close"

   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?

   if [ "${exitStatus}" != "0" ] ; then
       error "Curl command exited with non-zero status: ${exitStatus}"
       echo
       return 1
   fi

   parseQueryResponse

   if [ "${DW_QUERY_RESPONSE_CODE}" == 200 ] ; then
       if [ "${DW_QUERY_VERBOSE}" == true ] ; then
           prettyPrintResponse
       fi
       echo
       info "Query ${DW_QUERY_ID} closed"
       echo
   else
       prettyPrintResponse
   fi

   printCurlSummary
}

function getNextPage() {

   [ -z "${DW_QUERY_ID}" ] && error "DW_QUERY_ID is null. Can't retrieve results" && return 1
   [ -z "$( echo ${DW_QUERY_ID} | grep -E '^[a-zA-Z0-9\-]+$' )" ] && error "'${DW_QUERY_ID}' is not a valid query id" && return 1

   local curlcmd="/usr/bin/curl \
   --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
   --insecure --header 'Accept: application/json' ${DW_REQUEST_HEADERS} --cert ${DW_CURL_CERT} --key ${DW_CURL_KEY_RSA} --cacert ${DW_CURL_CA} --keepalive-time 180 \
   -X GET ${DW_QUERY_URI}/${DW_QUERY_ID}/next"

   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?

   if [ "${exitStatus}" != "0" ] ; then
       error "Curl command exited with non-zero status: ${exitStatus}"
       echo
       return 1
   fi

   parseQueryResponse

   if [ "${DW_QUERY_RESPONSE_CODE}" == 204 ] ; then
       echo
       info "End of result set, as indicated by response code '204'. Query will be closed automatically"
       echo
       printCurlSummary && closeQuery
       return 0
   fi

   if [ -n "${DW_QUERY_RESPONSE_BODY}" ] ; then
       prettyPrintResponse
   fi

   printCurlSummary
}

function parseQueryResponse() {
   DW_QUERY_RESPONSE_BODY=$( echo ${response} | sed -e 's/HTTP_STATUS_CODE\:.*//g' )
   DW_QUERY_RESPONSE_CODE=$( echo ${response} | tr -d '\n' | sed -e 's/.*HTTP_STATUS_CODE://' | sed -e 's/;TOTAL_TIME\:.*//' )
   DW_QUERY_RESPONSE_TYPE=$( echo ${response} | tr -d '\n' | sed -e 's/.*CONTENT_TYPE://' | sed -e 's/;.*//' )
   DW_QUERY_TOTAL_TIME=$( echo ${response} | tr -d '\n' | sed -e 's/.*TOTAL_TIME://' | sed -e 's/;CONTENT_TYPE\:.*//' )
}

function setCurlData() {
    # Concatenate function args into a list of -d args for curl
    DW_CURL_DATA="" ; for param in ${@} ; do
        DW_CURL_DATA="${DW_CURL_DATA} -d ${param}"
    done
}

function setQueryIdFromResponseXml() {
    DW_QUERY_ID=""

    # Parses the query id value from a query api response. E.g., from '/Query/{logicName}/create'
    # or from '/Query/{logicName}/createAndNext'

    # This will only work on responses having content-type == application/xml

    local id="$( echo ${1} | sed -e 's~<[?]xml .*><QueryId>\(.*\)</QueryId>.*~\1~' | sed -e 's~<[?]xml .*><Result .*>\(.*\)</Result>.*~\1~' )"

    # Filter out any unexpected input, only allow alphanumeric and hyphen chars

    id="$( echo ${id} | grep -E '^[a-zA-Z0-9\-]+$' )"

    [ -n "${id}" ] && DW_QUERY_ID=${id}
}

function setQueryIdFromResponseJson() {
    DW_QUERY_ID=""

    # Parses the query id value from a query api response. E.g., from '/Query/{logicName}/create'
    # or from '/Query/{logicName}/createAndNext'

    # This will only work on responses having content-type == application/json

    local id="$( echo "${1}" | sed -e 's~.*"QueryId":"\([a-zA-Z0-9\-]\+\)".*~\1~' | sed -e 's~.*"Result":"\([a-zA-Z0-9\-]\+\)"}.*~\1~' )"

    # Filter out any unexpected input, only allow alphanumeric and hyphen chars

    id="$( echo ${id} | grep -E '^[a-zA-Z0-9\-]+$' )"

    [ -n "${id}" ] && DW_QUERY_ID=${id}
}

function configureUserIdentity() {

    DW_PKI_TMP_DIR="${DW_DATAWAVE_DATA_DIR}"/pki-temp

    DW_PKCS12_CLIENT_CERT=${DW_PKCS12_CLIENT_CERT:-"${DW_DATAWAVE_SOURCE_DIR}"/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testUser.p12}
    DW_CLIENT_CERT_PASS=${DW_CLIENT_CERT_PASS:-ChangeIt}

    DW_CURL_CERT="${DW_PKI_TMP_DIR}/testUser.pem"
    DW_CURL_KEY="${DW_PKI_TMP_DIR}/testUser.key"
    DW_CURL_KEY_RSA="${DW_PKI_TMP_DIR}/testUser.key.rsa"
    DW_CURL_CA="${DW_PKI_TMP_DIR}/testUser.ca"

    [ -d "${DW_PKI_TMP_DIR}" ] && return 0 # Already configured

    mkdir -p "${DW_PKI_TMP_DIR}" || return 1

    info "Converting client certificate into more portable PKI materials. *Should* work no matter which versions you have of CURL, OpenSSL, NSS, etc"

    local OPENSSL="$( which openssl )" && [ -z "${OPENSSL}" ] && error "OpenSSL executable not found!" && return 1
    local opensslVersion3="$( ${OPENSSL} version | awk '{ print $2}' | grep -E ^3\. )"

    if [ -z "$opensslVersion3" ]; then
	local params="" # Not version 3.x
    else
	local params="-provider legacy -provider default" # Version 3.x
    fi

    [ ! -f "${DW_PKCS12_CLIENT_CERT}" ] && error "Source client certificate not found: ${DW_PKCS12_CLIENT_CERT}" && return 1

    ! ${OPENSSL} pkcs12 ${params} -passin "pass:${DW_CLIENT_CERT_PASS}" -passout "pass:${DW_CLIENT_CERT_PASS}" -in "${DW_PKCS12_CLIENT_CERT}" -out "${DW_CURL_KEY}" -nocerts > /dev/null 2>&1 && error "Key creation failed!" && return 1
    ! ${OPENSSL} rsa -passin "pass:${DW_CLIENT_CERT_PASS}" -in "${DW_CURL_KEY}" -out "${DW_CURL_KEY_RSA}" > /dev/null 2>&1 && error "RSA key creation failed!" && return 1
    ! ${OPENSSL} pkcs12 ${params} -passin "pass:${DW_CLIENT_CERT_PASS}" -in "${DW_PKCS12_CLIENT_CERT}" -out "${DW_CURL_CERT}" -clcerts -nokeys > /dev/null 2>&1 && error "Certificate creation failed!" && return 1
    ! ${OPENSSL} pkcs12 ${params} -passin "pass:${DW_CLIENT_CERT_PASS}" -in "${DW_PKCS12_CLIENT_CERT}" -out "${DW_CURL_CA}" -cacerts -nokeys > /dev/null 2>&1 && error "CA creation failed!" && return 1

    return 0
}

function reloadDataWaveTableCache() {

    # For convenience, this allows you to force a refresh of certain metadata caches in Wildfly, such as those used to cache 
    # DataWave's data element dictionary. Otherwise, you might be forced to bounce Wildfly or wait for the caches to reload 
    # automatically at the configured refresh interval.

    # This is particularly useful when you want to issue queries for newly ingested data, and to search on new, never-before-seen
    # field names that haven't yet arrived in Wildfly's mem cache.

    # Note that, by design, the reload endpoint invocation below is asynchronous, so there will still be at least a slight
    # delay before the refresh takes effect

    DW_CACHED_TABLE_NAMES=${DW_CACHED_TABLE_NAMES:-"datawave.metadata datawave.queryMetrics_m datawave.error_m"}

    local cachedTableNames=( ${DW_CACHED_TABLE_NAMES} )

    info "Reloading metadata table cache for the following tables: ${DW_CACHED_TABLE_NAMES}"

    configureUserIdentity

    for dwtable in "${cachedTableNames[@]}" ; do

        local curlcmd="/usr/bin/curl --silent --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" -X GET ${DW_BASE_URI}/Common/AccumuloTableCache/reload/${dwtable}"
        local response="$( eval "${curlcmd}" )"
        local exitStatus=$?

        if [ "${exitStatus}" != "0" ] ; then
            error "Curl command exited with non-zero status: ${exitStatus}. Failed to update table cache: ${dwtable}"
            return 1
        fi

   done
}
