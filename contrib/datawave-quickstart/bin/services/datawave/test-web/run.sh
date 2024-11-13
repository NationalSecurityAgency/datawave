#!/usr/bin/env bash

#
# This script automates testing of the DataWave REST API with a suite of preconfigured, curl-based tests.
# For each test, the following pass/fail assertion types are supported for expected vs actual responses:
# HTTP status code, HTTP content type, and HTTP body.
#
# Tests are pluggable, so new tests may be added without having to edit this script. See 'usage' and
# 'configure' methods below for more information on functionality and setup
#

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname $( dirname "${THIS_DIR}" ) )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

# Import some helpful bits from the quickstart environment...

source "${BIN_DIR}/env.sh"
source "${BIN_DIR}/query.sh"
source "${THIS_DIR}/../bootstrap.sh"

function usage() {
    echo
    echo "Usage: \$DW_DATAWAVE_SERVICE_DIR/test-web/$( printGreen "$( basename $0)" )"
    echo
    echo " Optional arguments:"
    echo
    echo " $( printGreen "-v" ) | $( printGreen "--verbose" )"
    echo "  Display each test's curl command and full web service response"
    echo "  Otherwise, only test- and response-related metadata are displayed"
    echo
    echo " $( printGreen "-n" ) | $( printGreen "--no-cleanup" )"
    echo "  Prevent cleanup of 'actual' HTTP response files and temporary PKI materials"
    echo "  Useful for post-mortem troubleshooting and/or manual testing"
    echo
    echo " $( printGreen "-c" ) | $( printGreen "--create-expected-responses" )"
    echo "  Update all *.expected HTTP response files to auto-configure body assertions"
    echo "  Or use in conjunction with whitelist/blacklist options to target only a subset"
    echo
    echo " $( printGreen "-lt" ) | $( printGreen "--list-tests" )"
    echo "  Print a listing of all tests grouped by test file name"
    echo
    echo " $( printGreen "-wf" ) | $( printGreen "--whitelist-files" ) F1,F2,..,Fn"
    echo "  Only execute test files having basename (minus .test extension) of F1 or F2 .. or Fn"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --blacklist-files, blacklist takes precedence"
    echo
    echo " $( printGreen "-bf" ) | $( printGreen "--blacklist-files" ) F1,F2,..,Fn"
    echo "  Exclude test files having basename (minus .test extension) of F1 or F2 .. or Fn"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --whitelist-files, blacklist takes precedence"
    echo
    echo " $( printGreen "-wt" ) | $( printGreen "--whitelist-tests" ) T1,T2,..,Tn"
    echo "  Only execute tests having TEST_UID of T1 or T2 .. or Tn"
    echo "  TEST_UID has the following format: 'basename of test file (minus .test extension)' + '/' + 'TEST_ID' e.g., TestFile/Test001"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --blacklist-tests, blacklist takes precedence"
    echo
    echo " $( printGreen "-bt" ) | $( printGreen "--blacklist-tests" ) T1,T2,..,Tn"
    echo "  Exclude tests having TEST_UID of T1 or T2 .. or Tn"
    echo "  TEST_UID has the following format: 'basename of test file (minus .test extension)' + '/' + 'TEST_ID' e.g., TestFile/Test001"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --whitelist-tests, blacklist takes precedence"
    echo
    echo " $( printGreen "-p" ) | $( printGreen "--pretty-print" )"
    echo "  Will format web service responses for readability, when used in conjunction with -v,--verbose"
    echo
    echo " $( printGreen "-er" ) | $( printGreen "--expected-response" ) TEST_UID"
    echo "  TEST_UID has the following format: 'basename of test file (minus .test extension)' + '/' + 'TEST_ID' e.g., TestFile/Test001"
    echo "  Print expected response metadata for the given test, along with pretty-printed example response body"
    echo
    echo " $( printGreen "-h" ) | $( printGreen "--help" )"
    echo "  Print this usage information and exit the script"
    echo
}

function configure() {

    HOST_PORT="${HOST_PORT:-localhost:8443}"
    URI_ROOT="https://${HOST_PORT}/DataWave"

    # TEST_FILE_DIR contains 'TestName.test' script files. These are sourced at runtime to set a few
    # required variables, to configure any test-specific curl args, and to execute any arbitrary code
    # that the test may need. See the 'runConfiguredTests' function below for details

    TEST_FILE_DIR=${THIS_DIR}/tests

    # RESP_FILE_DIR contains prepared, 'expected' HTTP response bodies for each test, for performing
    # assertions against 'actual' responses as needed. Actual response bodies are saved here temporarily
    # and removed during the cleanup phase. They may be retained via the --no-cleanup flag, if needed

    # Only the content type and status code assertions are configured manually via the test configs. The
    # assertion for the HTTP body is configured automatically via the --create-expected-responses flag.
    # See 'assertBodyResponse' method for details

    RESP_FILE_DIR=${THIS_DIR}/responses

    # Assertion status labels displayed for each test...

    LABEL_PASS="$( printGreen PASSED )"
    LABEL_FAIL="$( printRed FAILED )"
    LABEL_NA="DISABLED"

    # Process args...

    VERBOSE=false
    CLEANUP=true
    CREATE_EXPECTED=false
    TEST_WHITELIST=""
    TEST_BLACKLIST=""
    FILE_WHITELIST=""
    FILE_BLACKLIST=""
    PRETTY_PRINT=false
    LIST_TESTS=false
    LIST_ID=""

    while [ "${1}" != "" ]; do
       case "${1}" in
          --verbose | -v)
             VERBOSE=true
             ;;
          --no-cleanup | -n)
             CLEANUP=false
             ;;
          --create-expected-responses | -c)
             CREATE_EXPECTED=true
             ;;
          --whitelist-tests | -wt)
             TEST_WHITELIST="${2}"
             shift
             ;;
          --list-tests | -lt)
             LIST_TESTS=true
             ;;
          --expected-response | -er)
             LIST_TESTS=true
             LIST_ID="${2}"
             shift
             ;;
          --blacklist-tests | -bt)
             TEST_BLACKLIST="${2}"
             shift
             ;;
          --whitelist-files | -wf)
             FILE_WHITELIST="${2}"
             shift
             ;;
          --blacklist-files | -bf)
             FILE_BLACKLIST="${2}"
             shift
             ;;
          --pretty-print | -p)
             PRETTY_PRINT=true
             ;;
          --help | -h)
             usage && exit 0
             ;;
          *)
             fatal "Invalid argument passed to $( basename "$0" ): ${1}"
       esac
       shift
    done

    # Misc....

    datawaveWebIsInstalled || fatal "DataWave Web must be installed and running"

    datawaveWebIsRunning || fatal "DataWave Web must be running"

    CURL="$( which curl )" && [ -z "${CURL}" ] && fatal "Curl executable not found!"

    TEST_COUNTER=0

    configureUserIdentity || fatal "Failed to configure PKI"
}

function cleanup() {

    [ "$CLEANUP" != true ] && return
    info "Cleaning up temporary files"
    find "${RESP_FILE_DIR}" -type f -name "*.actual" -exec rm -f {} \;
}

function assert() {

    TEST_STATUS=""

    # Reset pass/fail indicators
    local codeAssertionLabel=${LABEL_PASS}
    local typeAssertionLabel=${LABEL_PASS}
    local bodyAssertionLabel=${LABEL_PASS}

    assertStatusCodeResponse
    assertContentTypeResponse
    assertBodyResponse

    if [ -z "${TEST_STATUS}" ] ; then
        TEST_STATUS="${LABEL_PASS}" && TESTS_PASSED="${TESTS_PASSED} ${TEST_UID}"
    else
        # List of failed tests, comma-delimited, and their assertion statuses
        TEST_FAILURES="${TEST_FAILURES},[${codeAssertionLabel}][${typeAssertionLabel}][${bodyAssertionLabel}] -> ${TEST_UID}"
    fi
}

function assertStatusCodeResponse() {

    if [ "${ACTUAL_RESPONSE_CODE}" != "${EXPECTED_RESPONSE_CODE}" ] ; then
        TEST_STATUS="$( echo "${TEST_STATUS}" && echo "[X] ${LABEL_FAIL} - Expected Code: '${EXPECTED_RESPONSE_CODE}' Actual: '${ACTUAL_RESPONSE_CODE}'" )"
        codeAssertionLabel="${LABEL_FAIL}"
    fi

}

function assertContentTypeResponse() {

    if [ "${ACTUAL_RESPONSE_TYPE}" != "${EXPECTED_RESPONSE_TYPE}" ] ; then
        TEST_STATUS="$( echo "${TEST_STATUS}" && echo "[X] ${LABEL_FAIL} - Expected Content-Type: '${EXPECTED_RESPONSE_TYPE}' Actual: '${ACTUAL_RESPONSE_TYPE}'" )"
        typeAssertionLabel="${LABEL_FAIL}"
    fi

}

function assertBodyResponse() {
    # Tests may enable/disable this assertion as needed via EXPECTED_RESPONSE_BODY_ASSERTION=true|false

    # Here the MD5 hash of the expected HTTP body is generated and compared to that of the actual
    # response's MD5, so no need to set expectations manually as with the status code and content type
    # assertions. If needed, you can create new or overwrite existing response files at any time with
    # the --create-expected-responses flag.

    EXPECTED_RESPONSE_FILE="${RESP_FILE_DIR}/${TEST_FILE_BASENAME}/${TEST_ID}.expected"
    ACTUAL_RESPONSE_FILE="${RESP_FILE_DIR}/${TEST_FILE_BASENAME}/${TEST_ID}.actual"

    local responseDir="$( dirname "${ACTUAL_RESPONSE_FILE}" )"

    if [[ ! -d "${responseDir}" ]] ; then
       mkdir -p "${responseDir}" || warn "Failed to create dir: ${responseDir}"
    fi

    # Write actual response to file

    echo -n "${ACTUAL_RESPONSE_BODY}" > "${ACTUAL_RESPONSE_FILE}"

    [ "${CREATE_EXPECTED}" == true ] && echo -n "${ACTUAL_RESPONSE_BODY}" > "${EXPECTED_RESPONSE_FILE}"

    # If the assertion is enabled, compare MD5 hashes

    if [ "${EXPECTED_RESPONSE_BODY_ASSERTION}" == true ] ; then
        EXPECTED_RESPONSE_BODY_MD5=$( md5sum "${EXPECTED_RESPONSE_FILE}" | cut -d ' ' -f1 )
        ACTUAL_RESPONSE_BODY_MD5=$( md5sum "${ACTUAL_RESPONSE_FILE}" | cut -d ' ' -f1 )
        if [ "${ACTUAL_RESPONSE_BODY_MD5}" != "${EXPECTED_RESPONSE_BODY_MD5}" ] ; then
            TEST_STATUS="$( echo "${TEST_STATUS}" && echo "[X] ${LABEL_FAIL} - Expected MD5: '${EXPECTED_RESPONSE_BODY_MD5}' Actual: '${ACTUAL_RESPONSE_BODY_MD5}'" )"
            bodyAssertionLabel="${LABEL_FAIL}"
        fi
    else
        bodyAssertionLabel="${LABEL_NA}"
    fi

}

function parseCurlResponse() {

    ACTUAL_RESPONSE_BODY=$( echo ${CURL_RESPONSE} | sed -e 's/HTTP_STATUS_CODE\:.*//g' )
    ACTUAL_RESPONSE_CODE=$( echo ${CURL_RESPONSE} | tr -d '\n' | sed -e 's/.*HTTP_STATUS_CODE://' | sed -e 's/;TOTAL_TIME\:.*//' )
    ACTUAL_RESPONSE_TYPE=$( echo ${CURL_RESPONSE} | tr -d '\n' | sed -e 's/.*CONTENT_TYPE://' )
    TOTAL_TIME=$( echo ${CURL_RESPONSE} | tr -d '\n' | sed -e 's/.*TOTAL_TIME://' | sed -e 's/;CONTENT_TYPE\:.*//' )

}

function runTest() {

    # Create unique id for the test
    TEST_UID="${TEST_FILE_BASENAME_NOEXT}/${TEST_ID}"

    # Filter the current TEST_UID based on whitelist/blacklist
    [ -n "$( echo "${TEST_BLACKLIST}" | grep -E "\b${TEST_UID}\b" )" ] && return
    [ -n "${TEST_WHITELIST}" ] && [ -z "$( echo "${TEST_WHITELIST}" | grep -E "\b${TEST_UID}\b" )" ] && return

    TEST_COUNTER=$((TEST_COUNTER + 1))

    TEST_COMMAND="${CURL} ${CURL_ADDITIONAL_OPTS} --silent \
--write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
--insecure --cert '${DW_CURL_CERT}' --keepalive-time 180 --key '${DW_CURL_KEY_RSA}' --cacert '${DW_CURL_CA}' ${TEST_URL_OPTS}"

    if [ "${LIST_TESTS}" == true ] ; then
        printCurrentTestInfo
        return
    fi

    echo
    echo "Test UID: ${TEST_UID}"
    echo "Test Description: ${TEST_DESCRIPTION}"
    if [ "$VERBOSE" == true ] ; then
        echo
        echo "Test Command:"
        echo "${TEST_COMMAND}"
    fi
    echo

    CURL_RESPONSE=$( eval "${TEST_COMMAND}" )
    CURL_EXIT_STATUS=$?

    if [ "${CURL_EXIT_STATUS}" != "0" ] ; then
        echo
        TEST_STATUS="${LABEL_FAIL} - Curl command exited with non-zero status: ${CURL_EXIT_STATUS}"
        TEST_FAILURES="${TEST_FAILURES},${TEST_UID} -> CURL_NON_ZERO_EXIT(${CURL_EXIT_STATUS})"
    else
        parseCurlResponse
        assert
        echo "HTTP Response Status Code: ${ACTUAL_RESPONSE_CODE}"
        echo "HTTP Response ContentType: ${ACTUAL_RESPONSE_TYPE}"
        if [ "$VERBOSE" == true ] ; then
            echo
            echo "HTTP Response Body:"
            if [ "$PRETTY_PRINT" == true ] ; then
                prettyPrintTestResponse
            else
                echo "${ACTUAL_RESPONSE_BODY}"
            fi
        fi
        echo
        [ "$VERBOSE" == true ] && echo "Test Finished: ${TEST_UID}"
        echo "Test Total Time: ${TOTAL_TIME}"
    fi

    echo "Test Status: ${TEST_STATUS}"

    echo
    printLine

    if [ "${1}" == "--set-query-id" ] ; then
       setQueryIdFromResponse
    fi
}

function printLine() {

    echo "$( printGreen "********************************************************************************************************" )"

}

function prettyPrintTestResponse() {
   echo
   if [ -n "${ACTUAL_RESPONSE_BODY}" ] ; then

       if [ "${ACTUAL_RESPONSE_TYPE}" == "application/json" ] ; then
           # defined in bin/query.sh
           prettyPrintJson "${ACTUAL_RESPONSE_BODY}"

       elif [ "${ACTUAL_RESPONSE_TYPE}" == "application/xml;charset=UTF-8" ] ; then
           # defined in bin/query.sh
           prettyPrintXml "${ACTUAL_RESPONSE_BODY}"
       else
           # Unknown type
           echo "${ACTUAL_RESPONSE_BODY}"
       fi
   else
       if [ "${ACTUAL_RESPONSE_CODE}" == "204" ] ; then
           info "No results for this query, as indicated by response code '204'"
       else
           info "No response body to print"
       fi
       echo
   fi
}

function printCurrentTestInfo() {

   if [ -n "${LIST_ID}" ] ; then

       # Printing expected response and info for a specific test id...

       if [ "${TEST_UID}" != "${LIST_ID}" ] ; then
           return
       fi

       ACTUAL_RESPONSE_BODY="$( cat "${RESP_FILE_DIR}/${TEST_FILE_BASENAME}/${TEST_ID}.expected" )"
       ACTUAL_RESPONSE_TYPE="${EXPECTED_RESPONSE_TYPE}"

       echo "$( printGreen "Example Response Body:" )"

       prettyPrintTestResponse

       echo "$( printGreen "Test File:" ) ${TEST_FILE_BASENAME}"
       echo "$( printGreen "Test ID:" ) ${TEST_ID}"
       echo "$( printGreen "Expected Type:" ) ${EXPECTED_RESPONSE_TYPE:-N/A}"
       echo "$( printGreen "Expected Code:" ) ${EXPECTED_RESPONSE_CODE}"
       echo "$( printGreen "Body Assertion Enabled": ) ${EXPECTED_RESPONSE_BODY_ASSERTION}"
       echo "$( printGreen "Test Description:" ) ${TEST_DESCRIPTION}"
       echo "$( printGreen "Test Curl Command:" ) ${TEST_COMMAND}"

   else

       # Printing info for all tests...
       echo "    ${TEST_COUNTER}$( printGreen ":" ) ${TEST_ID} | ${EXPECTED_RESPONSE_TYPE:-N/A} | ${EXPECTED_RESPONSE_CODE} | ${TEST_DESCRIPTION}"

   fi

}

function printTestSummary() {
    echo " Overall Summary"
    printLine
    echo
    echo " Test Count: ${TEST_COUNTER}"
    echo
    if [ -z "${TESTS_PASSED}" ] ; then
        echo " Tests Passed: 0"
    else
        local passed=(${TESTS_PASSED})
        echo "$( printGreen " Tests Passed: ${#passed[@]}" )"
        for p in "${passed[@]}" ; do
            echo "   ${p}"
        done
    fi
    echo
    if [ -z "${TEST_FAILURES}" ] ; then
        echo " Failed Tests: 0"
    else
        (
        IFS=","
        local failed=(${TEST_FAILURES})
        echo "$( printRed " Tests Failed: $(( ${#failed[@]} - 1 ))" )"
        echo " --------------------------------------------------------------------------------------------------"
        echo "  [Code Assertion][Type Assertion][Body Assertion] -> Failed Test ID (Test File)"
        echo " --------------------------------------------------------------------------------------------------"
        for f in "${failed[@]}" ; do
             echo "  ${f}"
        done
        )
        echo
        echo "  $( printYellow "Wildfly Logs:" ) \$WILDFLY_HOME/standalone/log"
        echo "  $( printYellow "Accumulo Logs:" ) \$ACCUMULO_HOME/logs"
        echo "  $( printYellow "Hadoop Logs:" ) \$HADOOP_HOME/logs"
        echo "  $( printYellow "Yarn Logs:" ) \$DW_CLOUD_DATA/hadoop/yarn/log"
    fi
    echo
    printLine
}

function exitWithTestStatus() {

    [ -n "${TEST_FAILURES}" ] && exit 1
    exit 0
}

function configureTest() {

  TEST_ID="${1}"
  TEST_DESCRIPTION="${2}"
  TEST_URL_OPTS="${3}"
  EXPECTED_RESPONSE_TYPE="${4}"
  EXPECTED_RESPONSE_CODE="${5}"
  EXPECTED_RESPONSE_BODY_ASSERTION="${6:-false}"
}

function configureCloseQueryTest() {

  configureTest \
    QueryCloseTest \
    "Closes the test query as necessary" \
    "-X PUT ${URI_ROOT}/Query/${1}/close" \
    "application/xml;charset=UTF-8" \
    200
}

function setQueryIdFromResponse() {

   DW_QUERY_ID=""

   case "${ACTUAL_RESPONSE_TYPE}" in
      application/json*)
         setQueryIdFromResponseJson "${ACTUAL_RESPONSE_BODY}"
         ;;
      application/xml*)
         setQueryIdFromResponseXml "${ACTUAL_RESPONSE_BODY}"
         ;;
      *)
         warn "Parsing query id from '${ACTUAL_RESPONSE_TYPE}' is not currently supported"
   esac

   [ -z "${DW_QUERY_ID}" ] && warn "Failed to parse a query id from \$ACTUAL_RESPONSE_BODY"
}

function runConfiguredTests() {

    # By design, each ${TEST_FILE_DIR}/*.test file may contain one or more distinct tests

    # At minimum, each distinct test within a file is defined by the following required variables:

    # TEST_ID                          - Identifier for the test. Internally, prefixed with file name to make TEST_UID
    # TEST_DESCRIPTION                 - Short text description of the test
    # TEST_URL_OPTS                    - Test-specific options for curl, e.g., "-X GET ${URI_ROOT}/DataDictionary"
    # EXPECTED_RESPONSE_TYPE           - Expected Content-Type for the response, e.g., "application/xml"
    # EXPECTED_RESPONSE_CODE           - Expected HTTP response code, e.g., 200, 404, 500, etc
    # EXPECTED_RESPONSE_BODY_ASSERTION - true/false, enables/disables expected vs actual HTTP body assertion

    # If a file contains multiple tests, then all but the *last* test within the file must be
    # followed by its own 'runTest' invocation. The last test in a file is always executed in
    # the for-loop below

    # Note that we process files in lexicographical order of filename. So, if needed, "C.test" can
    # reliably depend on tests "A.test" and "B.test" having already been run

    [ "${LIST_TESTS}" == false ] && printLine

    for TEST_FILE in $( find "${TEST_FILE_DIR}" -type f -name "*.test" | sort ) ; do

        TEST_FILE_BASENAME="$( basename "${TEST_FILE}" )"
        TEST_FILE_BASENAME_NOEXT="${TEST_FILE_BASENAME::-5}"

        # Filter the current *.test file based on whitelist/blacklist

        [ -n "$( echo "${FILE_BLACKLIST}" | grep "\b${TEST_FILE_BASENAME_NOEXT}\b" )" ] && continue

        [ -n "${FILE_WHITELIST}" ] && [ -z "$( echo "${FILE_WHITELIST}" | grep "${TEST_FILE_BASENAME_NOEXT}" )" ] && continue
        [ -n "${TEST_WHITELIST}" ] && [ -z "$( echo "${TEST_WHITELIST}" | grep -E "\b${TEST_FILE_BASENAME_NOEXT}/" )" ] && continue

        if [ "${LIST_TESTS}" == true ] && [ -z "${LIST_ID}" ] ; then
            echo "$( printGreen ${TEST_FILE_BASENAME_NOEXT} )"
        fi

        source "${TEST_FILE}" && runTest

    done
}

configure "$@"

runConfiguredTests

if [ "${LIST_TESTS}" != true ] ; then
   printTestSummary
   cleanup
   exitWithTestStatus
fi
