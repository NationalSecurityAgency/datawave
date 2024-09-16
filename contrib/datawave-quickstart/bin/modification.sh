#
# if using feature/queryMicroservices quickstart webserver:
#    DW_MODIFICATION_URI=https://localhost:9443/DataWave/Modification
#
# if using feature/queryMicroservices deployed modification service
#    DW_BASE_URI=-https://localhost:9443/DataWave
#    DW_MODIFICATION_URI=https://localhost:9343/modification/v1
#

DW_BASE_URI=${DW_BASE_URI:-https://localhost:8443/DataWave}
DW_MODIFICATION_URI=${DW_MODIFICATION_URI:-${DW_BASE_URI}/Modification}

function xmlencode() {
    local s
    s=${1//&/&amp;}
    s=${s//</&lt;}
    s=${s//>/&gt;}
    s=${s//'"'/&quot;}
    printf -- %s "$s"
}

function datawaveModification() {

   ! datawaveIsInstalled && info "DataWave Web is not installed. Try 'datawaveInstall'" && return 1
   ! datawaveWebIsRunning && info "DataWave Web is not running. Try 'datawaveWebStart'" && return 1

   # Reset

   DW_QUERY_RESPONSE_BODY=""
   DW_QUERY_RESPONSE_CODE=""
   DW_QUERY_RESPONSE_TYPE=""
   DW_QUERY_TOTAL_TIME=""

   # Both 'Content-Type: application/x-www-form-urlencoded' and 'Accept: application/json'
   # added by default, but may be overridden, if needed, via --header,-H option

   DW_REQUEST_HEADERS=""

   # Defaults

   DW_MODIFICATION_COMMAND="INSERT"
   DW_MODIFICATION_SERVICE="MutableMetadataUUIDService"
   DW_MODIFICATION_VIZ="BAR&FOO"
   DW_MODIFICATION_VERBOSE=false

   configureUserIdentity || return 1
   configureModification "$@" || return $?

   local curlcmd="/usr/bin/curl \
    --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
    --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" \
    --header 'Content-Type: application/xml;charset=UTF-8' --header 'Accept: application/xml' \
    ${DW_REQUEST_HEADERS} ${DW_CURL_DATA} -X PUT ${DW_MODIFICATION_URI}/${DW_MODIFICATION_SERVICE}/submit"
   echo $curlcmd

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
   printCurlSummary

   return 0
}

function configureModification() {

   while [ "${1}" != "" ]; do
      case "${1}" in
         --uuid | -u)
            DW_MODIFICATION_UUID="${2}"
            shift
            ;;
         --type | -t)
            DW_MODIFICATION_UUID_TYPE="${2}"
            shift
            ;;
         --field | -f)
            DW_MODIFICATION_FIELD="${2}"
            shift
            ;;
         --oldvalue | -o)
	    DW_MODIFICATION_OLD_VALUE="${2}"
            shift
            ;;
         --newvalue | -n)
            DW_MODIFICATION_NEW_VALUE="${2}"
            shift
            ;;
         --visibility | --vis)
            DW_MODIFICATION_VIZ="${2}"
            shift
            ;;
         --command | -c)
            DW_MODIFICATION_COMMAND="${2}"
	    shift
	    ;;
         --header | -H)
            DW_REQUEST_HEADERS="${DW_REQUEST_HEADERS} ${1} '${2}'"
            shift
            ;;
         --help | -h)
            modificationHelp && return 1
            ;;
         --verbose | -v)
            DW_MODIFICATION_VERBOSE=true
            ;;
         *)
            error "Invalid argument passed to $( basename "$0" ): ${1}" && return 1
      esac
      shift
   done

   [ -z "${DW_MODIFICATION_UUID}" ] && error "Uuid is required" && return 1
   [ -z "${DW_MODIFICATION_UUID_TYPE}" ] && error "Uuid type (field) is required" && return 1
   [ -z "${DW_MODIFICATION_FIELD}" ] && error "Field is required" && return 1
   [ -z "${DW_MODIFICATION_VIZ}" ] && error "Visibility is required" && return 1
   BODY="<DefaultUUIDModificationRequest xmlns=\"http://webservice.datawave/v1\" _class=\"datawave.webservice.modification.DefaultUUIDModificationRequest\"><Events><Event><id>${DW_MODIFICATION_UUID}</id><idType>${DW_MODIFICATION_UUID_TYPE}</idType><operations><operation><operationMode>${DW_MODIFICATION_COMMAND}</operationMode><fieldName>${DW_MODIFICATION_FIELD}</fieldName><fieldValue>${DW_MODIFICATION_NEW_VALUE}</fieldValue><columnVisibility>$( xmlencode ${DW_MODIFICATION_VIZ} )</columnVisibility></operation></operations><user>testUser</user></Event></Events><mode>INSERT</mode><fieldName>TEST</fieldName><fieldValue>ABC</fieldValue><columnVisibility>PUBLIC</columnVisibility></DefaultUUIDModificationRequest>"
   if [ "${DW_MODIFICATION_COMMAND}" == "INSERT" ] ; then
       [ -z "${DW_MODIFICATION_NEW_VALUE}" ] && error "New field value is required" && return 1
   elif [ "${DW_MODIFICATION_COMMAND}" == "REPLACE" ] ; then
       [ -z "${DW_MODIFICATION_NEW_VALUE}" ] && error "New field value is required" && return 1
   elif [ "${DW_MODIFICATION_COMMAND}" == "UPDATE" ] ; then
       [ -z "${DW_MODIFICATION_NEW_VALUE}" ] && error "New field value is required" && return 1
       [ -z "${DW_MODIFICATION_OLD_VALUE}" ] && error "Old field value is required" && return 1
       BODY="<DefaultUUIDModificationRequest xmlns=\"http://webservice.datawave/v1\" _class=\"datawave.webservice.modification.DefaultUUIDModificationRequest\"><Events><Event><id>${DW_MODIFICATION_UUID}</id><idType>${DW_MODIFICATION_UUID_TYPE}</idType><operations><operation><operationMode>${DW_MODIFICATION_COMMAND}</operationMode><fieldName>${DW_MODIFICATION_FIELD}</fieldName><fieldValue>${DW_MODIFICATION_NEW_VALUE}</fieldValue><oldFieldValue>${DW_MODIFICATION_OLD_VALUE}</oldFieldValue><columnVisibility>$( xmlencode ${DW_MODIFICATION_VIZ} )</columnVisibility></operation></operations><user>testUser</user></Event></Events><mode>INSERT</mode><fieldName>TEST</fieldName><fieldValue>ABC</fieldValue><columnVisibility>PUBLIC</columnVisibility></DefaultUUIDModificationRequest>"
   elif [ "${DW_MODIFICATION_COMMAND}" == "DELETE" ] ; then
       [ -z "${DW_MODIFICATION_OLD_VALUE}" ] && error "Old field value is required" && return 1
       BODY="<DefaultUUIDModificationRequest xmlns=\"http://webservice.datawave/v1\" _class=\"datawave.webservice.modification.DefaultUUIDModificationRequest\"><Events><Event><id>${DW_MODIFICATION_UUID}</id><idType>${DW_MODIFICATION_UUID_TYPE}</idType><operations><operation><operationMode>${DW_MODIFICATION_COMMAND}</operationMode><fieldName>${DW_MODIFICATION_FIELD}</fieldName><oldFieldValue>${DW_MODIFICATION_OLD_VALUE}</oldFieldValue><columnVisibility>$( xmlencode ${DW_MODIFICATION_VIZ} )</columnVisibility></operation></operations><user>testUser</user></Event></Events><mode>INSERT</mode><fieldName>TEST</fieldName><fieldValue>ABC</fieldValue><columnVisibility>PUBLIC</columnVisibility></DefaultUUIDModificationRequest>"
   else
       error "Command set to ${DW_MODIFICATION_COMMAND}.  Command must be one of INSERT, UPDATE, DELETE, or REPLACE." && return 1
   fi

   DW_CURL_DATA="-d '$BODY'"
}

function modificationHelp() {
    echo
    echo " The $( printGreen "datawaveModification" ) shell function allows you submit modification requests on demand to DataWave's"
    echo " Rest API and to inspect the results. It automatically configures curl and sets"
    echo " reasonable defaults for most required query parameters"
    echo
    echo " Assuming the following modification entries are in the datawave.metadata:"
    echo "   REVIEW m:csv []"
    echo "   REVIEW m:enwiki []"
    echo "   REVIEW m:tvmaze []"
    echo
    echo "       $( printGreen datawaveModification ) --uuid 09aa3d46-8aa0-49fb-8859-f3add48859b0 --type UUID --field REVIEW -c INSERT --newvalue 'I liked this one'"
    echo "       $( printGreen datawaveModification ) --uuid 09aa3d46-8aa0-49fb-8859-f3add48859b0 --type UUID --field REVIEW -c DELETE --oldvalue 'I liked this one'"
    echo "       $( printGreen datawaveModification ) --uuid 09aa3d46-8aa0-49fb-8859-f3add48859b0 --type UUID --field REVIEW -c REPLACE --newvalue 'I really liked this one'"
    echo "       $( printGreen datawaveModification ) --uuid 09aa3d46-8aa0-49fb-8859-f3add48859b0 --type UUID --field REVIEW -c UPDATE --oldvalue 'I liked this one' --newvalue 'I really liked this one'"
    echo
    echo " Required:"
    echo
    echo " $( printGreen "-u" ) | $( printGreen "--uuid" ) \"<uuid>\""
    echo "  The event uuid"
    echo
    echo " $( printGreen "-t" ) | $( printGreen "--type" ) \"<uuid type>\""
    echo "  The event uuid type (field)"
    echo
    echo " $( printGreen "-f" ) | $( printGreen "--field" ) \"<field>\""
    echo "  The field to modify"
    echo
    echo " Optional:"
    echo
    echo " $( printGreen "-c" ) | $( printGreen "--command" ) <syntax>"
    echo "  The command must be one of INSERT, UPDATE, DELETE, or REPLACE. Defaults to ${DW_MODIFICATION_COMMAND}"
    echo
    echo " $( printGreen "-n" ) | $( printGreen "--newvalue" ) <value>"
    echo "  The old value (required for INSERT, UPDATE or REPLACE service)"
    echo
    echo " $( printGreen "-o" ) | $( printGreen "--oldvalue" ) <value>"
    echo "  The old value (required for UPDATE or DELETE service)"
    echo
    echo " $( printGreen "--vis" ) | $( printGreen "--visibility" ) <visibility>"
    echo "  Visibility expression to use when logging this query to Accumulo. Defaults to '${DW_MODIFICATION_LOG_VIZ}'"
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

function listMutableFields() {

   # Reset

   DW_QUERY_RESPONSE_BODY=""
   DW_QUERY_RESPONSE_CODE=""
   DW_QUERY_RESPONSE_TYPE=""
   DW_QUERY_TOTAL_TIME=""
   DW_QUERY_EXTRA_PARAMS=""

   configureUserIdentity || return 1

   local curlcmd="/usr/bin/curl \
   --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
   --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" \
   -X GET ${DW_MODIFICATION_URI}/getMutableFieldList"

   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?
   if [ "${exitStatus}" != "0" ] ; then
       error "Curl command exited with non-zero status: ${exitStatus}"
       echo
       return 1
   fi

   parseQueryResponse
   prettyPrintResponse
   printCurlSummary
}

function reloadMutableFieldCache() {

   local curlcmd="/usr/bin/curl \
   --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
   --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" \
   -X GET ${DW_MODIFICATION_URI}/AccumuloTableCache/reload/datawave.metadata"
   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?

   if [ "${exitStatus}" != "0" ] ; then
       error "Curl command exited with non-zero status: ${exitStatus}. Failed to update table cache: ${dwtable}"
       return 1
   fi

   parseQueryResponse
   prettyPrintResponse
   printCurlSummary

   local curlcmd="/usr/bin/curl \
   --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
   --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" \
   -X GET ${DW_MODIFICATION_URI}/reloadCache"

   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?

   if [ "${exitStatus}" != "0" ] ; then
       error "Curl command exited with non-zero status: ${exitStatus}. Failed to update mutable fields cache: ${dwtable}"
       return 1
   fi

   parseQueryResponse
   prettyPrintResponse
   printCurlSummary
}

function listModificationConfiguration() {

   # Reset

   DW_QUERY_RESPONSE_BODY=""
   DW_QUERY_RESPONSE_CODE=""
   DW_QUERY_RESPONSE_TYPE=""
   DW_QUERY_TOTAL_TIME=""
   DW_QUERY_EXTRA_PARAMS=""

   configureUserIdentity || return 1

   local curlcmd="/usr/bin/curl \
   --silent --write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
   --insecure --cert "${DW_CURL_CERT}" --key "${DW_CURL_KEY_RSA}" --cacert "${DW_CURL_CA}" \
   -X GET ${DW_MODIFICATION_URI}/listConfigurations"

   local response="$( eval "${curlcmd}" )"
   local exitStatus=$?
   if [ "${exitStatus}" != "0" ] ; then
       error "Curl command exited with non-zero status: ${exitStatus}"
       echo
       return 1
   fi

   parseQueryResponse
   prettyPrintResponse
   printCurlSummary
}

