#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

MAX_ATTEMPTS=30
TIMEOUT=10

echo "Waiting for services to be ready..."

attempt=0
while [ $attempt -lt $MAX_ATTEMPTS ]; do
    echo "Checking query and executor status (${attempt}/${MAX_ATTEMPTS})"

    QUERY_STATUS=$(curl -s -m 5 http://localhost:8080/query/mgmt/health | grep UP)
    EXEC_STATUS=$(curl -s -m 5 http://localhost:8380/executor/mgmt/health | grep UP)
    if [ "${QUERY_STATUS}" == "{\"status\":\"UP\"}" ] && [ "${EXEC_STATUS}" == "{\"status\":\"UP\"}" ] ; then
        echo "Query and Executor Services running"
        break
    fi

    sleep ${TIMEOUT}

    ((attempt++))
done

echo "Running a test query..."

# Run the query, and capture the results
QUERY_RESULTS="$(${SCRIPT_DIR}/query.sh)"

echo "$QUERY_RESULTS"

SUCCESS="$(echo $QUERY_RESULTS | grep 'Page 1 contained 10 events' | grep 'Page 2 contained 2 events')"

if [ ! -z "${QUERY_RESULTS}" ] ; then
    echo "Query test passed"
    exit 0
fi

echo "Query test failed"
exit 1