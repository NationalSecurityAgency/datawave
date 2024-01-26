#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/query.sh

# QUERY PARAMETERS
QUERY_TYPE='errorDiscovery'
QUERY_LOGIC='ErrorDiscoveryQuery'
BEGIN='19660908 000000.000'
END='20301231 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='english'
QUERY_SYNTAX='LUCENE'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='100'

runQuery
