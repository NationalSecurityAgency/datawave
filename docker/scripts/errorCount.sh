#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/count.sh

# QUERY PARAMETERS
QUERY_TYPE='errorCount'
QUERY_LOGIC='ErrorCountQuery'
BEGIN='19660908 000000.000'
END='20301231 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='FOO_FIELD:myFoo'
QUERY_SYNTAX='LUCENE'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='10'

runCount
