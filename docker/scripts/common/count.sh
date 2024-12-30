#!/bin/bash

source ${SCRIPT_DIR}/common/query.sh

PAUSE='false'
MAX_PAGES=100
QUERY_TYPE='count'

# QUERY PARAMETERS
#QUERY_LOGIC='CountQuery'
#BEGIN='19660908 000000.000'
#END='20161002 235959.999'
#COLUMN_VISIBILITY='PUBLIC'
#QUERY='GENRES:[Action to Western]'
#QUERY_SYNTAX='LUCENE'
#AUTHS='PUBLIC,PRIVATE,BAR,FOO'
#QUERY_NAME='Developer Test Query'
#PAGE_SIZE='10'

# Override common get_num_events
get_num_events () {
    tag_found=false
    while read_dom; do
        if [[ $ENTITY =~ Field.*(RECORD_COUNT|count).* ]]; then
            tag_found=true
        elif [[ $tag_found == true ]]; then
            if [[ $ENTITY =~ 'Value' ]]; then
              echo $CONTENT
              break
            elif [[ $ENTITY =~ '/Field' ]]; then
                tag_found=false
            fi
        fi
    done
}

runCount() {
    runQuery
}