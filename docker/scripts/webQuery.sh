#!/bin/bash

# For this to work, the webserver must be running in the quickstart docker image.
# To do that, change --accumulo to --web or --webdebug in the docker-compose.yml.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/query.sh

# QUERY PARAMETERS
QUERY_TYPE='webQuery'
QUERY_LOGIC='RemoteEventQuery'
BEGIN='19660908 000000.000'
END='20161002 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='GENRES:[Action to Western]'
QUERY_SYNTAX='LUCENE'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='10'

# run query against the webservice
WEBSERVICE=true

runQuery
