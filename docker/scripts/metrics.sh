#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/query.sh

# QUERY PARAMETERS
QUERY_TYPE='metrics'
QUERY_LOGIC='QueryMetricsQuery'
BEGIN='20000101 000000.000'
END="$(date +%Y%m%d) 235959.999"
COLUMN_VISIBILITY='PUBLIC'
QUERY='QUERY_ID:[0 TO z]'
QUERY_SYNTAX='LUCENE'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='10'

runQuery
