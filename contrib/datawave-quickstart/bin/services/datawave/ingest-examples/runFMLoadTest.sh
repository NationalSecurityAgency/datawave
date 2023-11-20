#!/usr/bin/env bash

#========================================================================
# Purpose
# This provides a method of running the flag file load test. In lue of
# running using this script, the FlagMakerLoad class can be run from
# within Intellj or Eclipse. Two input parameters are required, the
# configuration file and the test jar that contains the FlagMakerLoad
# class.
#
# A default configuration file is located in the ingest-flag project
# (<ingest-flag>/src/test/resources/FlagLoadConfig.json). The path to
# the ingest files must be set properly. The tvmaze-flagmaker.sh script
# can be used to download the necessary ingest files but any set of
# ingest files will work.

function usage() {
    set +x
    test -n "$*" && echo "$@"
    echo "USAGE: ${_Argv0} -c config -j jar"
    echo -e "\tc: configuration file"
    echo -e "\tj: jar file"

    exit 1
}

function createJarClassPath() {
    local _fullCP=$1

    test -d "${_LibDir}" || {
        echo "${_LibDir} directory does not exist"
        exit 1
    }

    local _cp
    for j in $(find ${_LibDir} -name "*.jar"); do
        if [[ -n "${_cp}" ]]; then
            _cp=${_cp}:${j}
        else
            _cp=${j}
        fi
    done

    eval ${_fullCP}="${_cp}"
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++
declare -r _Argv0=$(basename $0)
declare -r _LibDir="../datawave-ingest-install/lib"

while getopts ":c:j:" _arg; do
    case ${_arg} in
        c) test -z "${_ConfigFile}" && declare -r _ConfigFile=$OPTARG;;
        j) test -z "${_RunJar}" && declare -r _RunJar=$OPTARG;;
        *) usage;;
    esac
done

test -z "${_ConfigFile}" -o -z "${_RunJar}" && usage
test -e "${_ConfigFile}" || usage "config file not found"
test -e "${_RunJar}" || usage "jar file not found"

declare _ClassPath
createJarClassPath _ClassPath

java -cp ${_RunJar}:${_ClassPath} datawave.util.flag.FlagMakerLoad ${_ConfigFile}
