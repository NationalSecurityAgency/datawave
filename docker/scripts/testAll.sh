#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

WEBSERVICE="${WEBSERVICE:-false}"
MAX_ATTEMPTS=30
TIMEOUT=10

# First argument is the script to run
# Second argument is the expected number of events
# Third argument is the expected number of pages/files
runTest () {
    ATTEMPTS=3
    ATTEMPT=1

    while [ $ATTEMPT -le $ATTEMPTS ]; do
        echo -n "Running test (Attempt ${ATTEMPT}/${ATTEMPTS}): $1 - "

        QUERY_RESPONSE="$(${SCRIPT_DIR}/$1)"

        if [[ "$QUERY_RESPONSE" == *"Returned $2 events"* ]] ; then
            if [ ! -z "$3" ] ; then
                if [[ "$QUERY_RESPONSE" == *"Returned $3 pages"* ]] ; then
                    echo "SUCCESS: Returned $2 events and $3 pages"
                    return 0
                elif [[ "$QUERY_RESPONSE" == *"Returned $3 files"* ]] ; then
                    echo "SUCCESS: Returned $2 events and $3 files"
                    return 0
                else
                    echo "FAILED: Unexpected number of pages/files returned"
                    echo
                    echo "TEST RESPONSE"
                    echo "$QUERY_RESPONSE"

                    sleep ${TIMEOUT}

                    if [ $ATTEMPT == $ATTEMPTS ] ; then
                        exit 1
                    fi
                fi
            else
                echo "SUCCESS: Returned $2 events"
                return 0
            fi
        else
            echo "FAILURE: Unexpected number of events returned"
            echo
            echo "TEST RESPONSE"
            echo "$QUERY_RESPONSE"

            sleep ${TIMEOUT}

            if [ $ATTEMPT == $ATTEMPTS ] ; then
                exit 1
            fi
        fi

        ((ATTEMPT++))
    done
}

if [ "$WEBSERVICE" = true ]; then
    echo "Waiting for webservice to be ready..."
else
    echo "Waiting for services to be ready..."
fi

attempt=0
while [ $attempt -lt $MAX_ATTEMPTS ]; do
    if [ "$WEBSERVICE" = true ]; then
        echo "Checking webservice status (${attempt}/${MAX_ATTEMPTS})"

        WEBSERVICE_STATUS=$(curl -s -m 5 -k https://localhost:8443/DataWave/Common/Health/health | grep Status)
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

    sleep ${TIMEOUT}

    ((attempt++))
done

if [ $attempt == $MAX_ATTEMPTS ]; then
    if [ "$WEBSERVICE" = true ]; then
        echo "FAILURE! Webservice never became ready"
    else
        echo "FAILURE! Query and/or Executor Services never became ready"
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

$SCRIPT_DIR/cleanup.sh

echo
echo "All tests SUCCEEDED!"
