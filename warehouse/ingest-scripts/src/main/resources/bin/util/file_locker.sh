#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
  THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
  THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

# ==============================================================
# ====================== Global Variables ======================
# ==============================================================
LOCKFILE_DIR="${lock.file.dir}/locks"
LOCKFILE_DIR_FD=755
LOG_FILE_PATH="${LOG_DIR}/script_flock_acquisition.log"
LOG_FD=664

################################################################
# Log that the file was already locked
# Globals:
#     LOG_FILE_PATH - The default path to the log file
# Arguments:
#     $1 - The name of the file trying to acquire the lock for
#     $2 - The path to the log file
# Returns:
#     NONE
################################################################
function log_locked() {
  local lock_file=$1
  local log_file=$2
  local fd=$LOG_FD

  touch $log_file
  chmod $fd $log_file

  printf "[WARNING] Unable to acquire lock on $lock_file\n" | tee -a $log_file
}

################################################################
# Make sure the directory to put the lock file into exists
# If it doesn't, create it
# Globals:
#     LOCKFILE_DIR - The directory to put the lock files into
#     LOCKFILE_DIR_FD - The file descriptor for the directory
# Arguments:
#     NONE
# Returns:
#     NONE
################################################################
function verify_lock_dir_exists() {
  if [ ! -d "$LOCKFILE_DIR" ]; then
    mkdir "$LOCKFILE_DIR"
    chmod "$LOCKFILE_DIR_FD" "$LOCKFILE_DIR"
  fi
}

################################################################
# Acquire/create the lock file
# Globals:
#     LOCKFILE_DIR - The location of the lock file
# Arguments:
#     $1 - The name of the lockfile
#     $2 - (optional) Overrides the default path to the log file
# Returns:
#     0 - If successfully acquired the lock file
#     1 - If unsuccessful in acquiring the lock file
################################################################
function lock() {
  local prefix=$1
  local log_file=${2:-$LOG_FILE_PATH}
  lock_file=$LOCKFILE_DIR/$prefix.lock # This will be accessible to the caller

  # Make sure lock directory exists
  verify_lock_dir_exists

  # Create lock file
  eval "exec {fd}>$lock_file"

  # Acquire the lock file
  # NOTE: If child processes are launched by script calling lock(), these child processes
  # will inherit the lock of the parent.
  # See `lsof +c 0 -a +d $LOCKFILE_DIR -u datawave -c "${prefix:0:15}"` for processes
  # holding a lock
  flock -n $fd \
    && return 0 \
    || (log_locked $lock_file $log_file; return 1)
}