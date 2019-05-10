#!/bin/bash

#=========================================================================
# Parses the output of the reducer into a CSV file when the
# "stats.reducer.counts" parameter to the reducer is set to true. To get
# valid data for analysis this also reqires the "stats.mapper.uniquecount"
# parameter to also be set to true.


function usage() {
    echo "USAGE: ${_Basename} [-v] reducerLogFile [... reducerLogFile]"

    exit 1
}

function createParseFile() {
    local -r _file=$1
    local -r _out=$2

    # find all relevant output records, removing all 0 entries
    egrep "add values| reduce key" ${_file} | grep -v "err(0.0)" > ${_out}
    echo -en "\tfield count: "
    grep "reduce key" ${_out} | wc -l
    echo -en "\tshard entries: "
    grep "add values" ${_out} | wc -l
}

function verbose() {
    test -n "${_Verbose}" && {
        echo -e "$*"
    }
}

function createCSV() {
    local -r _file=$1
    local -r _csv=${_file%.*}.csv

    echo "csv file: ${_csv}"

    # create CSV header
    echo "Field Name, Date, Datatype, Total Count, LogPlus Cardinality, Actual Unique, LogPlus Selectivity, Actual Selectivity, Diff, Err %" >${_csv}
    cat ${_file} | while read _data; do
        local _field
        local _date
        local _type
        local -i _fieldTotal=0

        echo ${_data} | grep -q "reduce key"
        if [[ $? -eq 0 ]]; then
            # set new field/date/type info
            local _key=${_data##*key(}
            set -- ${_key}
            _field=$1
            _date=${2%:*}
            _type=${2#*:}
            if [[ 0 -lt "${_fieldTotal}" ]]; then
                verbose "\tfield entries: ${_fieldTotal}"
            fi
            verbose "\tnew field/type: field(${_field}) type(${_type})"
            _fieldTotal=0
        else
            # process shard info
            local _values=${_data##*StatsHyperLogSummary\{}
            # strip characters from value string
            _values=${_values//(}
            _values=${_values//)}
            _values=${_values//\}}

            # split value string
            set -- ${_values}
            local _count=${1#*nt}
            local _card=${3#*ity}
            local _cardSel=${5#*select}
            local _unique=${6#*que}
            local _actualSel=${8#*select}
            local _diff=${9#*diff}
            local _err=${10#*err}
            ((_fieldTotal = _fieldTotal + 1))
            echo "${_field}, ${_date}, ${_type}, ${_count}, ${_card}, ${_unique}, ${_cardSel}, \
${_actualSel}, ${_diff}, ${_err}" >>${_csv}
        fi
    done
}

#======================================================
declare -r _Basename=$(basename $0)

while getopts ":v" _arg; do
    case ${_arg} in
        v) shift
            test -z "${_Verbose}" && declare _Verbose=true;;
        *) usage;;
    esac
done

declare _pFile
test -z "$*" && usage
for _inFile in $*; do
    echo "reducer input file: ${_inFile}"
    _pFile=${_inFile%.*}.parsed
    createParseFile ${_inFile} ${_pFile}
    createCSV ${_pFile}
done
