#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/query.sh

# QUERY PARAMETERS
QUERY_TYPE='termFrequency'
QUERY_LOGIC='TermFrequencyQuery'
BEGIN='19500101 000000.000'
END='20161002 235959.999'
COLUMN_VISIBILITY='PUBLIC'
QUERY='jackie:19520920_0/tvmaze/-bb3qxp.e771of.e3f2gs'
QUERY_SYNTAX='LUCENE'
AUTHS='PUBLIC,PRIVATE,BAR,FOO'
QUERY_NAME='Developer Test Query'
PAGE_SIZE='10'

runQuery
