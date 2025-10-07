#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/batchLookup.sh

# QUERY PARAMETERS
BEGIN='19660908 000000.000'
END='20161002 235959.999'
COLUMN_VISIBILITY='PUBLIC'
UUID_PAIRS="PAGE_TITLE:anarchism OR PAGE_TITLE:accessiblecomputing"
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Lookup UUID Query'

runBatchLookup
