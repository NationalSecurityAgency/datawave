#!/usr/bin/env bash
#
# Utility function to call the `pdsh` command and log the results via `dshbak`
#
# NOTE: In order to use this script, calling scripts need to make sure that
# ingest-env.sh has been sourced. Since calling scripts may source ingest-env.sh
# with the -force option, calling without that option could be a potential issue
# as it could overwrite variables incorrectly.

##################################################################
# Call the bash `pdsh` command and log results to the specified
# log directory using `dshbak`
# Globals:
#     $PDSH_LOG_DIR - The base directory to place results in
# Usage:
#     logging_pdsh <script_name> [-cmd <cmd_name>] <pdsh_args>...
##################################################################
function logging_pdsh() {
  script_name=$1
  localhost=$(hostname -s)
  pid=$$

  if [[ $2 == "-cmd" ]]; then
    command_name=$3
    PDSH_OUT="${PDSH_LOG_DIR}/${script_name}/${pid}/${command_name}"
    pdsh_args="${@:4}"  # Skip over first 3 args
  else
    PDSH_OUT="${PDSH_LOG_DIR}/${script_name}/${pid}"
    pdsh_args="${@:2}"  # Skip over first arg
  fi

  rm -rf ${PDSH_OUT}
  pdsh ${pdsh_args} < /dev/null \
    1> >(dshbak -f -d ${PDSH_OUT}/stdout) \
    2> >(dshbak -f -d ${PDSH_OUT}/stderr); \
    cat ${PDSH_OUT}/stderr/pdsh\@${localhost} 2> /dev/null

}