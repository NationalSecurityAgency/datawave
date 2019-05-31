#!/usr/bin/env bash

#========================================================================
# Purpose
# Downloads json files from TVMaze for testing of the FlagMaker. The
# FlagMaker load test uses multiple threads that select files at random.
# The number of ingest files should be modified to conform to the test
# requirements.

function usage() {
    set +x
    echo "USAGE: ${_Argv0} -c -s [-d dir] [-n count]"
    echo -e "\tc: download cast data"
    echo -e "\td: output directory for TVMaze files (default => ${_DefaultOutputDir})"
    echo -e "\tn: number of entries to download (default => ${_DefaultEntryCount})"
    echo -e "\ts: download show data"

    exit 1
}

#=============================================================
# Performs the download of the specified URL to a local file.
function download() {
    local -r _urlSuffix=$1
    local -r _outFile=$2

    local -r _url=${_TVMAZE_API}/${_urlSuffix}
    local -i _retry=5
    while [[ ${_retry} -gt 0 ]]; do
        local _code=$(curl --silent --output ${_outFile} --write-out '%{http_code}' -X GET "${_url}")
        local _rc=$?
        test $_rc -eq 0 && return
        # check for rate limiting
        if [[ "${_code}" -eq 429 ]]; then
            ((_retry = _retry - 1))
            if [[ "${_retry}" -gt 0 ]]; then
                sleep 1
            else
                echo "ERROR: rate limiting retry failure for url(${_url})"
            fi
        else
            echo "ERROR: ${_url} => http code(${_code}) retry for url(${_url})"
            return
        fi
    done
}

#=============================================================
# Downloads shows from the TVmaze site.
function downloadShows() {
    local -ir _max=$1

    local -i _num=1
    local -ir _Interval=50
    while [[ "${_num}" -le "${_max}" ]]; do
        download "shows/${_num}" "shows-${_num}.json"
        ((_num = _num + 1))
        local -i _mod
        ((_mod = _num % _Interval))
        test "${_mod}" -eq 0 && echo "current show count ($_num)"
    done
   echo "show download total(${_max})"
}

#=============================================================
# Downloads shows with cast from the TVmaze site.
function downloadCast() {
   local -ir _max=$1

   local -i _num=1
   local -ir _Interval=50
   while [[ "${_num}" -le "${_max}" ]]; do
        download "shows/${_num}/cast" "shows-cast-${_num}.json"
        ((_num = _num + 1))
        local -i _mod
        ((_mod = _num % _Interval))
        test "${_mod}" -eq 0 && echo "current cast count ($_num)"
   done
   echo "cast download total(${_max})"
}

#================================================

declare -r _Argv0=$(basename $0)
declare -r _TVMAZE_API="http://api.tvmaze.com"
declare -r _DefaultOutputDir=tvmaze
declare -r _DefaultEntryCount=2500

while getopts ":cd:n:s" _arg; do
    case $_arg in
        c) test -z "${_Cast}" && _Cast=true;;
        d) test -z "${_TVMazeDir}" && _TVMazeDir=${OPTARG};;
        n) test -z "${_Num}" && _Num=${OPTARG};;
        s) test -z "${_Shows}" && _Shows=true;;
        *) usage;;
    esac
done

test -z "${_TVMazeDir}" && _TVMazeDir=${_DefaultOutputDir}
test -d "${_TVMazeDir}" || {
    mkdir -p ${_TVMazeDir} || exit 1
}
cd ${_TVMazeDir} || exit 1
test -z "${_Num}" && _Num=${_DefaultEntryCount}

echo "download directory => ${_TVMazeDir}"
echo "download count (${_Num})"

test -n "${_Shows}" && downloadShows ${_Num}
test -n "${_Cast}" && downloadCast ${_Num}
