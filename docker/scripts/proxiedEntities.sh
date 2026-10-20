#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common/common.sh"

readonly CERT="${SCRIPT_DIR}/../pki/testServer.crt"
readonly KEY="${SCRIPT_DIR}/../pki/testServer.key"
readonly ISSUER="CN=EXAMPLE CORP CA, O=Example Corp, C=US"
readonly USER_ONE="CN=proxy user one, OU=Example Developers, O=Example Corp, C=US"
readonly USER_TWO="CN=proxy user two, OU=Example Developers, O=Example Corp, C=US"
readonly SERVER_ONE="CN=proxy server one, OU=Servers, O=Example Corp, C=US"
readonly SERVER_TWO="CN=proxy server two, OU=Servers, O=Example Corp, C=US"
readonly WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

run_case() {
    local label="$1"
    local user="$2"
    local servers="$3"
    local auths="$4"
    local expected="$5"
    local subjects="<${user}>"
    local issuers="<${ISSUER}>"
    local server
    local response="${WORK_DIR}/${label}.xml"
    local status

    for server in ${servers}; do
        case "${server}" in
            one) subjects+="<${SERVER_ONE}>" ;;
            two) subjects+="<${SERVER_TWO}>" ;;
        esac
        issuers+="<${ISSUER}>"
    done

    status=$(curl -sS -k --cert "${CERT}" --key "${KEY}" \
        -H "Accept: application/xml" \
        -H "Pool: pool1" \
        -H "X-ProxiedEntitiesChain: ${subjects}" \
        -H "X-ProxiedIssuersChain: ${issuers}" \
        --data-urlencode "begin=20160426 000000.000" \
        --data-urlencode "end=20160426 235959.999" \
        --data-urlencode "columnVisibility=A" \
        --data-urlencode "query=FOO_FIELD == 'PROXIED_AUTH_CASE'" \
        --data-urlencode "query.syntax=JEXL" \
        --data-urlencode "auths=${auths}" \
        --data-urlencode "systemFrom=proxiedEntities.sh" \
        --data-urlencode "queryName=${label}" \
        --data-urlencode "pagesize=100" \
        "${DATAWAVE_ENDPOINT}/EventQuery/createAndExecute" \
        -o "${response}" -w '%{http_code}')

    if [[ "${expected}" == "reject" ]]; then
        if [[ "${status}" != "400" ]]; then
            echo "FAIL ${label}: expected HTTP 400, got ${status}"
            cat "${response}"
            return 1
        fi
        if ! grep -Fq "User requested authorizations" "${response}"; then
            echo "FAIL ${label}: HTTP 400 was not caused by an unauthorized auth request"
            cat "${response}"
            return 1
        fi
    else
        if [[ "${status}" != "200" ]]; then
            echo "FAIL ${label}: expected HTTP 200, got ${status}"
            cat "${response}"
            return 1
        fi
        python3 "${SCRIPT_DIR}/common/checkProxiedEntitiesResponse.py" "${response}" "${expected}" || return 1
    fi

    echo "PASS ${label}"
}

# The caller certificate is the transport server. The two NPEs below are
# distinct entities in the chain and have different mock authorization sets.
run_case "one-user-intersection" "${USER_ONE}" "one" "A,B,C" "COMMON,USER_ONE_SERVER_ONE"
run_case "one-user-user-only-filtered" "${USER_ONE}" "one" "A,B,C,D" "COMMON,USER_ONE_SERVER_ONE"
run_case "one-user-npe-overreports" "${USER_ONE}" "one" "A,B,C,G" "reject"

# A second user makes a separate request through the same NPE.
run_case "second-user-same-npe-intersection" "${USER_TWO}" "one" "A,B,E" "COMMON,USER_TWO_SERVER_ONE"
run_case "second-user-same-npe-user-only-filtered" "${USER_TWO}" "one" "A,B,E,F" "COMMON,USER_TWO_SERVER_ONE"
run_case "second-user-same-npe-overreports" "${USER_TWO}" "one" "A,B,E,G" "reject"

run_case "two-npes-intersection" "${USER_ONE}" "one two" "A,B" "COMMON"
run_case "two-npes-second-filtered" "${USER_ONE}" "one two" "A,B,C" "COMMON"
run_case "two-npes-npe-overreports" "${USER_ONE}" "one two" "A,B,C,G" "reject"
