
# Shared utility code used by tests as needed

function setData() {
    # Concatenate function args into a list of -d args for curl
    DATA="" ; for param in ${@} ; do
        DATA="${DATA} -d ${param}"
    done
}

function setQueryIdFromResponseXml() {
    QUERY_ID=""

    # Parses the query id value from a query api response. E.g., from '/Query/{logicName}/create'
    # or from '/Query/{logicName}/createAndNext'

    # This will only work on responses having content-type == application/xml where the query
    # id value appears as the inner text of either a <Result> or a <QueryId> element

    # Any other response type or format will likely fail, in which case QUERY_ID will remain null/empty

    local id="$( echo ${ACTUAL_RESPONSE_BODY} | sed -e 's~<[?]xml .*><QueryId>\(.*\)</QueryId>.*~\1~' | sed -e 's~<[?]xml .*><Result .*>\(.*\)</Result>.*~\1~' )"

    # Filter out any unexpected input, only allow alphanumeric and hyphen chars

    id="$( echo ${id} | grep -E '^[a-zA-Z0-9\-]+$' )"

    [ -n "${id}" ] && QUERY_ID=${id}
}

function urlencode() {
	local LANG=C i c e=''
	for (( i=0; i < ${#1}; i++ )); do
        c=${1:$i:1}
		[[ "$c" =~ [a-zA-Z0-9\.\~\_\-] ]] || printf -v c '%%%02X' "'$c"
        e+="$c"
	done
    echo "$e"
}
