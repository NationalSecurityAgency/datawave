
# Shared utility code used by tests as needed

function setData() {
    DATA="" ; for param in ${@} ; do
        DATA="${DATA} -d ${param}"
    done
}

function printQueryCreateID() {
    # Parses/prints the query id from a '/Query/{logicName}/create' response having
    # content-type == application/xml. Will likely fail on any other input

    local id="$( echo ${1} | sed -e 's~<[?]xml .*><Result .*>\(.*\)</Result>.*~\1~' )"

    # In the event of unexpected input, ensure that only alphanumeric and hyphen chars are
    # allowed through

    id="$( echo ${id} | grep -E '^[a-zA-Z0-9\-]+$' )"

    echo "${id}"
}

