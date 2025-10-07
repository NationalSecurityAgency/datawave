#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

WEBSERVICE="${WEBSERVICE:-false}"
MAX_ATTEMPTS=30
QUERY_TIMEOUT=2m
# Amount of time to wait after a failed test attempt
TIMEOUT=10
# Amount of time to wait for services to be ready
SERVICES_INTERVAL=4
TEST_COUNTER=0

# First argument is the script to run
# Second argument is the expected number of events
# Third argument is the expected number of pages/files
runTest () {
    ATTEMPTS=3
    ATTEMPT=1
    TEST_COUNTER=$((TEST_COUNTER + 1))
    test_start_time=$(date +%s%N)

    while [ $ATTEMPT -le $ATTEMPTS ]; do
        echo
        echo -n "Running test (Attempt ${ATTEMPT}/${ATTEMPTS}): $1 - "
        echo

        attempt_start_time=$(date +%s%N)
        QUERY_RESPONSE="$(timeout ${QUERY_TIMEOUT} ${SCRIPT_DIR}/$1)"
        attempt_end_time=$(date +%s%N)
        EXIT_CODE=$?

        if [[ "$QUERY_RESPONSE" == *"Returned $2 events"* ]] ; then
            if [ ! -z "$3" ] ; then
                if [[ "$QUERY_RESPONSE" == *"Returned $3 pages"* ]] ; then
                    TEST_STATUS="${LABEL_PASS} -> Returned $2 events and $3 pages" && TESTS_PASSED="${TESTS_PASSED} $1"
                    printTestStatus "$attempt_start_time" "$attempt_end_time" "$TEST_STATUS"
                    printLine
                    return 0
                elif [[ "$QUERY_RESPONSE" == *"Returned $3 files"* ]] ; then
                    TEST_STATUS="${LABEL_PASS} -> Returned $2 events and $3 files" && TESTS_PASSED="${TESTS_PASSED} $1"
                    printTestStatus "$attempt_start_time" "$attempt_end_time" "$TEST_STATUS"
                    printLine
                    return 0
                else
                    TEST_STATUS="${LABEL_FAIL} -> Unexpected number of pages/files returned: Expected $2 events and $3 files."
                    echo "Query Response:"
                    echo "$QUERY_RESPONSE"
                    echo "----------------"

                    if [ $ATTEMPT == $ATTEMPTS ] ; then
                        TEST_STATUS="${LABEL_FAIL} -> Failed to succeed after ${ATTEMPT} attempts"
                        TEST_FAILURES="${TEST_FAILURES},${1}: ${TEST_STATUS}"
                        printTestStatus "$test_start_time" "$(date +%s%N)" "$TEST_STATUS"
                        printLine
                        return 1
                    else
                        sleep ${TIMEOUT}
                    fi
                fi
            else
                TEST_STATUS="${LABEL_PASS} -> Returned $2 events" && TESTS_PASSED="${TESTS_PASSED} $1"
                printTestStatus "$attempt_start_time" "$attempt_end_time" "$TEST_STATUS"
                printLine
                return 0
            fi
        else
            if [ $EXIT_CODE == 124 ] ; then
                TEST_STATUS="${LABEL_FAIL} -> Query timed out after ${QUERY_TIMEOUT}"
                printTestStatus "$attempt_start_time" "$attempt_end_time" "$TEST_STATUS"
            else
                TEST_STATUS="${LABEL_FAIL} -> Unexpected number of events returned: Expected $2 events."
                printTestStatus "$attempt_start_time" "$attempt_end_time" "$TEST_STATUS"
                echo "Query Response:"
                echo "$QUERY_RESPONSE"
                echo "----------------"
            fi

            if [ $ATTEMPT == $ATTEMPTS ] ; then
                TEST_STATUS="${LABEL_FAIL} -> Failed to succeed after ${ATTEMPT} attempts"
                TEST_FAILURES="${TEST_FAILURES},${1}: ${TEST_STATUS}"
                printTestStatus "$test_start_time" "$(date +%s%N)" "$TEST_STATUS"
                printLine
                return 1
            else
                sleep ${TIMEOUT}
            fi
      fi
      ((ATTEMPT++))
    done
}

printTestSummary() {
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
        for f in "${failed[@]}" ; do
            echo "  ${f}"
        done
        )
    fi
    echo
    printLine
}

setPrintColors
setTestLabels

if [ "$WEBSERVICE" = true ]; then
    echo "Waiting for webservice to be ready..."
else
    echo "Waiting for services to be ready..."
fi

attempt=0
while [ $attempt -lt $MAX_ATTEMPTS ]; do
    if [ "$WEBSERVICE" = true ]; then
        echo "Checking webservice status (${attempt}/${MAX_ATTEMPTS})"

        WEBSERVICE_STATUS=$(curl -s -m 5 -k https://localhost:9443/DataWave/Common/Health/health | grep Status)
        if [[ "${WEBSERVICE_STATUS}" =~ \"Status\":\"ready\" ]] ; then
            echo "Webservice ready"
            break
        fi
    else
        echo "Checking query and executor status (${attempt}/${MAX_ATTEMPTS})"

        QUERY_STATUS=$(curl -s -m 5 http://localhost:8080/query/mgmt/health | grep UP)
        EXEC_STATUS=$(curl -s -m 5 http://localhost:8380/executor/mgmt/health | grep UP)
        if [ "${QUERY_STATUS}" == "{\"status\":\"UP\"}" ] && [ "${EXEC_STATUS}" == "{\"status\":\"UP\"}" ] ; then
            echo "Query and Executor Services ready"
            break
        fi
    fi

    sleep ${SERVICES_INTERVAL}

    ((attempt++))
done

if [ $attempt == $MAX_ATTEMPTS ]; then
    if [ "$WEBSERVICE" = true ]; then
        echo "$( printRed "FAILURE" ) - Webservice never became ready"
    else
        echo "$( printRed "FAILURE" ) - Query and/or Executor Services never became ready"
    fi
    exit 1
fi

echo "Running tests..."
echo

runTest batchLookup.sh 2
runTest batchLookupContent.sh 4
runTest count.sh 12 1
runTest discovery.sh 2 1
# runTest edge.sh 0 0
# runTest edgeEvent.sh 1 1
runTest errorCount.sh 1 1
runTest errorDiscovery.sh 1 1
runTest errorFieldIndexCount.sh 1 1
runTest errorQuery.sh 1 1
runTest fieldIndexCount.sh 12 2
runTest hitHighlights.sh 12 2
runTest lookup.sh 1
runTest lookupContent.sh 2
# runTest metrics.sh 0 0
runTest query.sh 12 2
#runTest mapReduceQuery.sh 12 2
#runTest oozieQuery.sh 0 0

# Gives option to skip the cleanup stage
if [ "${1}" == "-noCleanup" ] ; then
    printTestSummary
    exit 0
fi

printTestSummary
# The cleanup script will only delete the logs for tests that passed. Failed test logs will remain.
"$SCRIPT_DIR"/cleanup.sh "${TESTS_PASSED}"