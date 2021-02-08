#!/usr/bin/env bash
#
# Utility functions to cleanup old files on the cluster.

# Since this script is only sourced, normal THIS_DIR logic won't work.
THIS_DIR=`dirname ${BASH_SOURCE}`
cd ${THIS_DIR}

. ../ingest/ingest-env.sh

# ===================================================================
# ===================== Global Variables ============================
# ===================================================================

# If the location of the path is on HDFS
LOC_HDFS="hdfs"

# If the location of the path is on local disk
LOC_LOCAL="local"

# The name of the file to log success/failures in
LOG_FILE="${LOG_DIR}/cleanup-old-files.log"

function log() {
    local __message=$1
    local __path=$2
    local __path_location=$3
    [[ ${__path} == */ ]] && __path="${__path::-1}"  # Remove trailing "/"

    local __day_cutoff=$4
    local __gender=$5

    printf '%s\n' \
        "$(date) ${__message}" \
        " -- PATH = '${__path}'" \
        " -- LOC = '${__path_location}'" \
        " -- DAY_CUTOFF = ${__day_cutoff}" \
        " -- GENDER = '${__gender}'" >> ${LOG_FILE}
}

###################################################################
# Log the deletion was successful
# Globals:
#     $LOG_FILE - The name of the file to log success in
# Arguments:
#     $1 - The path to the files we deleted
#     $2 - Filesystem where the path is located (local or HDFS)
#     $3 - How old the file must be for it to be up-for-deletion
#     $4 - The gender of the servers to run the command on
###################################################################
function log_success() {
    log "[SUCCESS] Successfully removed all files matching the following criteria:" "$@"
}

###################################################################
# Log that the file removal failed
# Globals:
#     $LOG_FILE - The name of the file to log failures in
# Arguments:
#     $1 - The path to the files we attempted to remove
#     $2 - Filesystem where the path is located (local or HDFS)
#     $3 - How old the file must be for it to be up-for-deletion
#     $4 - The gender of the servers to run the command on
#     $5 - The number of files leftover after performing the removal
#     $6 - The error code of the failed command
###################################################################
function log_failure() {
    log "[ERROR] Error while removing files matching the following criteria { leftover files: $5, error code: $6 }:" "$@"
}

###################################################################
# Get the number of files older than $4 days
# Globals:
#     $LOC_HDFS - The specified path is on HDFS
#     $LOC_LOCAL - The specified path is on local disk
#     $LOG_FILE - The name of the file to log success/fail in
# Arguments:
#     $1 - The path to the files we're attempting to remove
#     $2 - Filesystem where the path is located (local or HDFS)
#     $3 - [DOES NOT WORK IF HDFS LOCATION] The type of file to remove (see `man find` -type arg)
#     $4 - How old the file must be for it to be counted
#     $5 - The gender of the servers to run the command on
# Returns:
#     The number of files we're attempting to remove (as an integer to stdout)
###################################################################
function get_count() {
    local __path=$1
    local __path_location=$2
    [[ ${__path} == */ ]] && __path="${__path::-1}"  # Remove trailing "/"

    local __file_type=$3
    local __day_cutoff=$4
    local __gender=$5

    # Perform the count based on the path location
    local __count=0
    if [[ ${__path_location} == ${LOC_HDFS} ]]; then
        __count=$(hadoop fs -ls -R ${__path} | egrep -v "^d" | tr -s " " | awk -v pastDate=$(date "+%Y-%m-%d" --date="${__day_cutoff} days ago") '$6 < pastDate' 2>/dev/null | wc -l | tr -d "\n")
    elif [[ ${__path_location} == ${LOC_LOCAL} ]]; then
        if [[ -z ${__gender} ]]; then
            __count=$(find ${__path}/* -type ${__file_type} -mtime +${__day_cutoff} 2>/dev/null | wc -l | tr -d "/n")
        else
            __count=$(pdsh -f 6 -b -g "${__gender}" "find ${__path}/* -type ${__file_type} -mtime +${__day_cutoff}" 2>/dev/null | wc -l | tr -d "/n")
        fi
    else
        log "[ERROR] Invalid argument passed to get_count: ${__path_location}. Must be ${LOC_HDFS} or ${LOC_LOCAL}." \
            "${__path}" "${__path_location}" ${__day_cutoff} "${__gender}"
    fi

    echo $((${__count} + 0))  # To force a conversion from string to integer and place the result in stdout
}

###################################################################
# Remove all files of the specified type older than $4 days
# Globals:
#     $LOC_HDFS - The specified path is on HDFS
#     $LOC_LOCAL - The specified path is on local disk
#     $LOG_FILE - Path of the file to log to
# Arguments:
#     $1 - The path to the files we're attempting to remove
#     $2 - Filesystem where the path is located (local or HDFS)
#     $3 - [DOES NOT WORK IF HDFS LOCATION] The type of file to remove (see `man find` -type arg)
#     $4 - How old the file must be for it to be up-for-removal
#     $5 - The gender of the servers to run the command on
###################################################################
function remove_old_files() {
    local __path=$1
    local __path_location=$2
    [[ ${__path} == */ ]] && __path="${__path::-1}"  # Remove trailing "/"

    local __file_type=$3
    local __day_cutoff=$4
    local __gender=$5

    if [[ ${__path_location} == ${LOC_HDFS} ]]; then
        hadoop fs -ls -R ${__path} | egrep -v "^d" | tr -s " " | awk -v pastDate=$(date "+%Y-%m-%d" --date="${__day_cutoff} days ago") '{if($6 < pastDate) system("hdfs dfs -rm -f "$NF)}' >> ${LOG_FILE}
    elif [[ ${__path_location} == ${LOC_LOCAL} ]]; then
        # Determine correct delete command
        local __delete_cmd=$([[ ${__file_type} == "d" ]] && echo "-exec rm -rv {} +" || echo "-delete")

        if [[ -z ${__gender} ]]; then
            find ${__path}/* -type ${__file_type} -mtime +${__day_cutoff} ${__delete_cmd} 2>/dev/null >> ${LOG_FILE}
        else
            pdsh -f 6 -b -g "${__gender}" "find ${__path}/* -type ${__file_type} -mtime +${__day_cutoff} ${__delete_cmd}" 2>/dev/null >> ${LOG_FILE}
        fi
    else
        log "[ERROR] Invalid argument passed to remove_old_files: ${__path_location}. Must be ${LOC_HDFS} or ${LOC_LOCAL}." \
            "${__path}" "${__path_location}" ${__day_cutoff} "${__gender}"
    fi

    # Variables to represent the result of the deletion
    local __result=$?
    local __leftover=$(get_count "$@")

    # If command was a success and there were no leftover files
    if [[ ${__result} -eq 0 && ${__leftover} -eq 0 ]]; then
        # Log successful action
        log_success "${__path}" "${__path_location}" ${__day_cutoff} "${__gender}"
        return 0
    else
        # Log the error
        log_failure "${__path}" "${__path_location}" ${__day_cutoff} "${__gender}" ${__leftover} ${__result}
        [[ ${__result} -eq 0 ]] && return 1 || return ${__result}
    fi
}