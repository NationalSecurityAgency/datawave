#!/bin/bash
#
# Cleanup old pdsh logs on the cluster

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
    THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd ${THIS_DIR}

. ../util/cleanup_old_files_util.sh

# =================================================================
# ===================== Global Variables ==========================
# =================================================================

# The (default) number of days the file must be older than to be up-for-removal
DAY_CUTOFF=7

function print_usage() {
    echo "Usage:"
    echo "    cleanup_old_pdsh_logs.sh [-d DAYS]"
    echo
    echo "Options:"
    echo "    -d DAYS    The number of days the file must be older than to be removed (Default is ${DAY_CUTOFF})"
    echo

    exit 1
}

# =================================================================
# ===================== Global Variables ==========================
# =================================================================

# Get the value of the optional number of days argument
if [[ $# -ne 0 ]]; then
    if [[ $# -ne 2 ]] || [[ $1 != "-d" ]]; then
        print_usage
    fi

    DAY_CUTOFF=$2
fi

# Begin the cleanup of the pdsh logs older than ${DAY_CUTOFF} days
echo "$(date) [INFO] Beginning cleanup of pdsh logs on the system older than ${DAY_CUTOFF} days" >> ${LOG_FILE}
remove_old_files ${PDSH_LOG_DIR} ${LOC_LOCAL} d ${DAY_CUTOFF} ""
result=$?

printf "$(date) [INFO] Finished cleanup of pdsh logs on the system older than ${DAY_CUTOFF} days\n\n" >> ${LOG_FILE}
exit ${result}
