#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/query.sh

# QUERY PARAMETERS
QUERY_TYPE='hitHighlights'
QUERY_LOGIC='HitHighlights'
BEGIN='19660908 000000.000'
END='20161002 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='GENRES:[Action to Western]'
QUERY_SYNTAX='LUCENE'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='10'

runQuery
