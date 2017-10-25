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
source "${THIS_DIR}/../bootstrap.sh"

# Any shared utility code needed by the tests themselves should
# be added to ${THIS_DIR}/common.sh, rather than to this script...

source "${THIS_DIR}/common.sh"

function usage() {
    echo
    echo "Usage: \$DW_DATAWAVE_SERVICE_DIR/test-web/$( printGreen "$( basename $0)" )"
    echo "       Or the $( printGreen "datawaveWebTest" ) shell function may be used as an alias"
    echo
    echo " Optional arguments:"
    echo
    echo " $( printGreen "-v" ) | $( printGreen "--verbose" )"
    echo "  Display each test's curl command and full web service response"
    echo "  Otherwise, only test- and response-related metadata are displayed"
    echo
    echo " $( printGreen "-n" ) | $( printGreen "--no-cleanup" )"
    echo "  Prevent cleanup of 'actual' HTTP response files and temp PKI materials"
    echo "  Useful for post-mortem troubleshooting and/or manual testing"
    echo
    echo " $( printGreen "-c" ) | $( printGreen "--create-expected-responses" )"
    echo "  Update all the *.expected HTTP response body files in order to"
    echo "  auto-configure their respective test assertions"
    echo
    echo " $( printGreen "-wf" ) | $( printGreen "--whitelist-files" ) F1.test,F2.test,..,Fn.test"
    echo "  Only execute test files having basename of F1.test or F2.test .. or Fn.test"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --blacklist-files, blacklist takes precedence"
    echo
    echo " $( printGreen "-bf" ) | $( printGreen "--blacklist-files" ) F1.test,F2.test,..,Fn.test"
    echo "  Exclude test files having basename of F1.test or F2.test .. or Fn.test"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --whitelist-files, blacklist takes precedence"
    echo
    echo " $( printGreen "-wt" ) | $( printGreen "--whitelist-tests" ) T1,T2,..,Tn"
    echo "  Only execute tests having TEST_ID of T1 or T2 .. or Tn"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --blacklist-tests, blacklist takes precedence"
    echo
    echo " $( printGreen "-bt" ) | $( printGreen "--blacklist-tests" ) T1,T2,..,Tn"
    echo "  Exclude tests having TEST_ID of T1 or T2 .. or Tn"
    echo "  If more than one, must be comma-delimited with no spaces"
    echo "  If used in conjunction with --whitelist-tests, blacklist takes precedence"
    echo
    echo " $( printGreen "-h" ) | $( printGreen "--help" )"
    echo "  Print this usage information and exit the script"
    echo
}

function configure() {

    HOST_PORT="localhost:8443"
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

    LABEL_PASS="PASSED"
    LABEL_FAIL="FAILED"
    LABEL_NA="DISABLED"

    # Process args...

    VERBOSE=false
    CLEANUP=true
    CREATE_EXPECTED=false
    TEST_WHITELIST=""
    TEST_BLACKLIST=""
    FILE_WHITELIST=""
    FILE_BLACKLIST=""

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

    configureUserIdentity
}

function configureUserIdentity() {

    info "Converting client certificate into more curl-friendly & portable (hopefully) PKI materials"

    PKCS12_CLIENT_CERT=${PKCS12_CLIENT_CERT:-"${DW_DATAWAVE_SOURCE_DIR}"/web-services/deploy/application/src/main/wildfly/overlay/standalone/configuration/certificates/testUser.p12}
    CLIENT_CERT_PASS=${CLIENT_CERT_PASS:-secret}

    PKI_TMP_DIR="${DW_DATAWAVE_DATA_DIR}"/pki-temp

    CURL_CERT="${PKI_TMP_DIR}/testUser.pem"
    CURL_KEY="${PKI_TMP_DIR}/testUser.key"
    CURL_KEY_RSA="${PKI_TMP_DIR}/testUser.key.rsa"
    CURL_CA="${PKI_TMP_DIR}/testUser.ca"

    [ -d "${PKI_TMP_DIR}" ] && info "Temporary directory already exists: ${PKI_TMP_DIR}. Will use existing PKI materials" && return

    mkdir -p "${PKI_TMP_DIR}"

    OPENSSL="$( which openssl )" && [ -z "${OPENSSL}" ] && fatal "OpenSSL executable not found!"

    [ ! -f "${PKCS12_CLIENT_CERT}" ] && fatal "Source client certificate not found: ${PKCS12_CLIENT_CERT}"

    ${OPENSSL} pkcs12 -passin pass:${CLIENT_CERT_PASS} -passout pass:${CLIENT_CERT_PASS} -in "${PKCS12_CLIENT_CERT}" -out "${CURL_KEY}" -nocerts > /dev/null 2>&1 || fatal "Key creation failed!"
    ${OPENSSL} rsa -passin pass:${CLIENT_CERT_PASS} -in "${CURL_KEY}" -out "${CURL_KEY_RSA}" > /dev/null 2>&1 || fatal "RSA key creation failed!"
    ${OPENSSL} pkcs12 -passin pass:${CLIENT_CERT_PASS} -in "${PKCS12_CLIENT_CERT}" -out "${CURL_CERT}" -clcerts -nokeys > /dev/null 2>&1 || fatal "Certificate creation failed!"
    ${OPENSSL} pkcs12 -passin pass:${CLIENT_CERT_PASS} -in "${PKCS12_CLIENT_CERT}" -out "${CURL_CA}" -cacerts -nokeys > /dev/null 2>&1 || fatal "CA creation failed!"

}

function cleanup() {

    [ "$CLEANUP" != true ] && echo && return

    info "Cleaning up temporary files"

    [ -d "${PKI_TMP_DIR}" ] && rm -rf "${PKI_TMP_DIR}"

    rm -f "${RESP_FILE_DIR}"/*.actual

    echo
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
        TEST_STATUS="${LABEL_PASS}" && TESTS_PASSED="${TESTS_PASSED} ${TEST_ID}"
    else
        # Build a list of failed tests and their assertion statuses to display condensed summary at the end
        TEST_FAILURES="${TEST_FAILURES} [${codeAssertionLabel}][${typeAssertionLabel}][${bodyAssertionLabel}]->${TEST_ID}"
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

    EXPECTED_RESPONSE_FILE="${RESP_FILE_DIR}/${TEST_ID}.expected"
    ACTUAL_RESPONSE_FILE="${RESP_FILE_DIR}/${TEST_ID}.actual"

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

    # Filter the current TEST_ID based on whitelist/blacklist
    [ -n "$( echo "${TEST_BLACKLIST}" | grep -E "\b${TEST_ID}\b" )" ] && return
    [ -n "${TEST_WHITELIST}" ] && [ -z "$( echo "${TEST_WHITELIST}" | grep -E "\b${TEST_ID}\b" )" ] && return

    TEST_COUNTER=$((TEST_COUNTER+1))

    TEST_COMMAND="${CURL} ${CURL_ADDITIONAL_OPTS} --silent \
--write-out 'HTTP_STATUS_CODE:%{http_code};TOTAL_TIME:%{time_total};CONTENT_TYPE:%{content_type}' \
--insecure --cert '${CURL_CERT}' --key '${CURL_KEY_RSA}' --cacert '${CURL_CA}' ${TEST_URL_OPTS}"

    echo
    #printLine
    echo "Test ID: ${TEST_ID}"
    echo "Test Description: ${TEST_DESCRIPTION}"
    echo "Test File: $( basename "${TEST_FILE}" )"
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
        TEST_FAILURES="${TEST_FAILURES} ${TEST_ID}->CURL_NON_ZERO_EXIT(${CURL_EXIT_STATUS})"
    else

        parseCurlResponse
        assert
        echo "HTTP Response Status Code: ${ACTUAL_RESPONSE_CODE}"
        echo "HTTP Response ContentType: ${ACTUAL_RESPONSE_TYPE}"
        if [ "$VERBOSE" == true ] ; then
            echo
            echo "HTTP Response Body:"
            echo "${ACTUAL_RESPONSE_BODY}"
        fi
        echo
        echo "Test Finished: ${TEST_ID}"
        echo "Test Total Time: ${TOTAL_TIME}"

    fi

    if [ "${TEST_STATUS}" == "${LABEL_PASS}" ] ; then
        echo "Test Status: $( printGreen "${TEST_STATUS}" )"
    else
        echo "Test Status: $( printRed "${TEST_STATUS}" )"
    fi

    echo
    printLine
}

function printLine() {

    echo "$( printGreen "********************************************************************************************************" )"

}

function printTestFailure() {
    #echo "${1}"
    local addSpaces="$( echo "${1}" | sed "s/->/ -> /" )"
    #echo "${addSpaces}"
    local formatPassFail="$( echo "${addSpaces}" | sed "s/${LABEL_PASS}/\$(printGreen ${LABEL_PASS}\)/g" | sed "s/${LABEL_FAIL}/\$(printRed ${LABEL_FAIL}\)/g" )"
    #echo "${formatPassFail}"
    echo -n "  "
    eval "echo \"${formatPassFail}\""
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
        local failed=(${TEST_FAILURES})
        echo "$( printRed " Tests Failed: ${#failed[@]}" )"
        echo " ------------------------------------------------------------------------------------------------------"
        echo "  [StatusCode Assertion][ContentType Assertion][Body Assertion] $( printGreen "${LABEL_PASS}")|$( printRed "${LABEL_FAIL}" )|${LABEL_NA} -> FailedTestID"
        echo " ------------------------------------------------------------------------------------------------------"
        for f in "${failed[@]}" ; do
            printTestFailure "${f}"
        done
    fi
    echo
    printLine
}

function exitWithTestStatus() {
    [ -n "${TEST_FAILURES}" ] && exit 1
    exit 0
}

function runConfiguredTests() {

    # By design, each ${TEST_FILE_DIR}/*.test file may contain one or more distinct tests

    # At minimum, each distinct test within a file is defined by the following required variables:

    # TEST_ID                          - Unique identifier for the test
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

    printLine

    for TEST_FILE in $( find "${TEST_FILE_DIR}" -type f -name "*.test" | sort ) ; do

        # Filter the current *.test file based on whitelist/blacklist

        [ -n "$( echo "${FILE_BLACKLIST}" | grep "$( basename ${TEST_FILE} )" )" ] && continue

        [ -n "${FILE_WHITELIST}" ] && [ -z "$( echo "${FILE_WHITELIST}" | grep "$( basename ${TEST_FILE} )" )" ] && continue

        source "${TEST_FILE}" && runTest
    done

}

configure "$@"

runConfiguredTests

printTestSummary

cleanup

exitWithTestStatus
