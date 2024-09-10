#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/edge.sh

# QUERY PARAMETERS
QUERY_LOGIC='EdgeQuery'
BEGIN='19660908 000000.000'
END='20161002 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='SOURCE == 'Jerry Seinfeld''
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Edge Query'
PAGE_SIZE='100'

runEdgeQuery
