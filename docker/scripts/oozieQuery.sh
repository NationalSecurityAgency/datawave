#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/oozieQuery.sh

# QUERY PARAMETERS
WORKFLOW='OozieJob'
COLUMN_VISIBILITY='PUBLIC'

runOozieQuery
