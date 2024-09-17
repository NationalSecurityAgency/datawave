#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/query.sh

# QUERY PARAMETERS
QUERY_LOGIC='EventQuery'
BEGIN='19660908 000000.000'
END='20161002 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='geowave:intersects(FIELD, "POLYGON((-30 -30, 30 -30, 30 30, -30 30, -30 -30))") AND GENRES >= "Action" && GENRES <= "Western"'
QUERY_SYNTAX='JEXL'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='10'

runQuery