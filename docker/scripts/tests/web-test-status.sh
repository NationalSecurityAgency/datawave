#!/usr/bin/env bash
# Exercise the unchanged web runner without quickstart setup, services, or network.
set -eu
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
runner=contrib/datawave-quickstart/bin/services/datawave/test-web/run.sh
root=$(mktemp -d "${TMPDIR:-/tmp}/web-test-status.XXXXXX")
trap 'command rm -rf -- "$root"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
checks=0 failures=0 cases=0

printGreen() { printf '%s\n' "$*"; }
printRed() { printGreen "$@"; }
printYellow() { printGreen "$@"; }
info() { printGreen "$@"; }
warn() { printGreen "$@"; }
fatal() { printGreen "$@" >&2; return 1; }
configureUserIdentity() { return 0; }
export -f printGreen printRed printYellow info warn fatal configureUserIdentity

contains() { [[ $1 == *"$2"* ]]; }
check() {
    local message=$1
    shift
    checks=$((checks + 1))
    if ! "$@"; then
        printf 'FAIL [%s] %s\n' "$label" "$message"
        failures=$((failures + 1))
    fi
}

run_case() {
    local label=$1 code=$2 expected=$3 mode=$4 status=0 output summary id
    local sandbox="$root/$label" before=$failures
    local bin="$sandbox/contrib/datawave-quickstart/bin" web="$sandbox/${runner%/*}"
    local -a args=() responses=()
    cases=$((cases + 1))
    mkdir -p -- "$web/tests" "$web/responses" "$sandbox/mock-bin"
    cp -- "$repo/$runner" "$sandbox/$runner"
    : > "$bin/env.sh"
    : > "$bin/query.sh"
    : > "$bin/services/datawave/bootstrap.sh"
    printf '%s\n' '#!/usr/bin/env bash' \
        '[[ " $* " == *" https://localhost:9443/DataWave/fixture "* ]] || exit 99' \
        "printf '%s\\n' '<response/>HTTP_STATUS_CODE:200;TOTAL_TIME:0;CONTENT_TYPE:application/xml'" \
        > "$sandbox/mock-bin/curl"
    chmod +x "$sandbox/mock-bin/curl"
    printf '%s\n' \
        'configureTest First "Configurable status" "${URI_ROOT}/fixture" application/xml "${FIXTURE_EXPECTED_CODE:-599}"' \
        'runTest' \
        'configureTest Later "Later passing test" "${URI_ROOT}/fixture" application/xml 200' \
        > "$web/tests/Status.test"
    printf '%s\n' 'configureTest Skipped "Blacklisted failure" "${URI_ROOT}/fixture" application/xml 599' \
        > "$web/tests/QueryMetrics.test"
    [[ $mode != no-cleanup ]] || args=(--no-cleanup)
    (
        cd -- "$sandbox"
        env -u BASH_ENV -u ENV -u SHELLOPTS -u BASHOPTS -u TEST_FAILURES -u TESTS_PASSED \
            PATH="$sandbox/mock-bin:$PATH" FIXTURE_EXPECTED_CODE="$code" \
            HOST_PORT=localhost:9443 DW_DATAWAVE_WEB_REMOTE=true \
            bash "$runner" --blacklist-files QueryMetrics "${args[@]}"
    ) > "$sandbox/output" 2>&1 || status=$?
    output=$(<"$sandbox/output")
    summary=${output#*Overall Summary}
    check "exit $status, expected $expected" test "$status" -eq "$expected"
    check 'summary present' contains "$output" 'Overall Summary'
    check 'summary printed once' test "${summary/Overall Summary/}" = "$summary"
    check 'both tests counted' contains "$summary" 'Test Count: 2'
    check 'later passing test summarized' contains "$summary" '   Status/Later'
    check 'blacklisted test absent' test "${output/QueryMetrics/}" = "$output"
    if ((expected)); then
        check 'one passed' contains "$summary" 'Tests Passed: 1'
        check 'one failed' contains "$summary" 'Tests Failed: 1'
        check 'first failure summarized' contains "$summary" '[FAILED][PASSED][DISABLED] -> Status/First'
        check 'deliberate mismatch reported' contains "$output" "Expected Code: '599' Actual: '200'"
    else
        check 'both passed' contains "$summary" 'Tests Passed: 2'
        check 'no failures' contains "$summary" 'Failed Tests: 0'
        check 'first passing test summarized' contains "$summary" '   Status/First'
    fi
    shopt -s nullglob globstar
    responses=("$web/responses"/**/*.actual)
    if [[ $mode == no-cleanup ]]; then
        check 'both responses retained' test "${#responses[@]}" -eq 2
        for id in First Later; do
            check "$id response retained" test -s "$web/responses/Status.test/$id.actual"
        done
    else
        check 'all actual responses removed' test "${#responses[@]}" -eq 0
    fi
    if ((failures == before)); then printf 'PASS [%s]\n' "$label"; else printf '%s\n' "$output"; fi
}

for mode in default no-cleanup; do
    run_case "failure-$mode" 599 1 "$mode"
    run_case "success-$mode" 200 0 "$mode"
done
printf '\n%s cases, %s assertions, %s failures\n' "$cases" "$checks" "$failures"
((failures == 0))
