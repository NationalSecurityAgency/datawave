#!/usr/bin/env bash
# Run the full production runner with exported fixtures, never real queries/services.
set -u
scripts=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || exit 2
root=$(mktemp -d "${TMPDIR:-/tmp}/testAll-status.XXXXXX") || exit 2
trap 'command rm -rf -- "$root"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
suite=(batchLookup batchLookupContent count discovery errorCount errorDiscovery
       errorFieldIndexCount errorQuery fieldIndexCount hitHighlights lookup lookupContent query)
checks=0 failures=0 cases=0

curl() { printf '%s\n' '{"status":"UP"}'; }
sleep() { return 0; }
timeout() {
    local name=${2##*/} events pages='' kind=pages calls=0 response artifact
    name=${name%.sh}
    case "$name" in
        batchLookup|lookupContent) events=2 ;;
        batchLookupContent) events=4 ;;
        lookup) events=1 ;;
        count) events=12; pages=1 ;;
        discovery) events=2; pages=1; kind=files ;;
        errorCount|errorDiscovery|errorFieldIndexCount|errorQuery) events=1; pages=1 ;;
        fieldIndexCount|hitHighlights|query) events=12; pages=2 ;;
        *) printf 'Unexpected fixture query: %s\n' "$name" >&2; return 99 ;;
    esac
    [[ ! -f "$FIXTURE_ROOT/.calls/$name" ]] || read -r calls < "$FIXTURE_ROOT/.calls/$name"
    ((calls+=1))
    printf '%s\n' "$calls" > "$FIXTURE_ROOT/.calls/$name"
    if [[ $FIXTURE_SCENARIO == counts ]]; then
        case "$name" in batchLookup) events=999 ;; count|discovery) pages=999 ;; esac
    elif [[ $FIXTURE_SCENARIO == retry && $name == query && $calls == 1 ]]; then
        events=999
    fi
    artifact="$FIXTURE_ROOT/${name}_fixture"
    command mkdir -p -- "$artifact" || return 99
    printf -v response 'Returned %s events\n' "$events"
    [[ -z $pages ]] || printf -v response '%sReturned %s %s\n' "$response" "$pages" "$kind"
    printf '%s' "$response" > "$artifact/querySummary.txt"
    printf '%s' "$response"
    if [[ " $CHILD_TESTS " == *" $name "* ]]; then return "$CHILD_STATUS"; fi
    return 0
}
rm() {
    printf 'called\n' >> "$FIXTURE_ROOT/.cleanup.calls"
    if [[ $CLEANUP_STATUS != 0 ]]; then return "$CLEANUP_STATUS"; fi
    command rm "$@"
}
export -f curl sleep timeout rm

check() {
    local message=$1
    shift
    ((checks+=1))
    if ! "$@"; then
        printf 'FAIL [%s] %s\n' "$label" "$message"
        ((failures+=1))
    fi
}
contains() { [[ $1 == *"$2"* ]]; }
diagnostic() {
    local pattern=$1 line
    while IFS= read -r line; do
        [[ $line =~ $pattern ]] && return 0
    done <<< "$output"
    return 1
}

# Arguments: label, cleanup mode, fixture scenario, expected exit, failed names,
# cleanup status, child names, child status. Each case gets a fresh sandbox.
run_case() {
    local label=$1 mode=$2 scenario=$3 expected=$4 failed=$5
    local sandbox="$root/$label" status=0 output summary name calls attempts kept present
    local before=$failures failed_count=0
    local -a args=() failed_names=()
    ((cases+=1))
    command mkdir -p -- "$sandbox/common" "$sandbox/.calls" || exit 2
    command cp -- "$scripts/testAll.sh" "$scripts/cleanup.sh" "$sandbox/" || exit 2
    command cp -- "$scripts/common/common.sh" "$sandbox/common/" || exit 2
    command chmod +x "$sandbox/cleanup.sh" || exit 2
    export FIXTURE_ROOT="$sandbox" FIXTURE_SCENARIO="$scenario"
    export CLEANUP_STATUS="${6:-0}" CHILD_TESTS="${7:-}" CHILD_STATUS="${8:-0}"
    [[ $mode != noCleanup ]] || args=(-noCleanup)
    # Strip inherited runner state and shell startup/options; retain fixture functions.
    (
        cd -- "$sandbox" || exit 2
        command env -u BASH_ENV -u ENV -u SHELLOPTS -u BASHOPTS \
            -u TEST_FAILURES -u TESTS_PASSED -u TEST_COUNTER -u EXIT_CODE \
            WEBSERVICE=false bash ./testAll.sh "${args[@]}"
    ) > "$sandbox/output" 2>&1 || status=$?
    output=$(<"$sandbox/output")
    output=${output//$'\e[31m'/}
    output=${output//$'\e[32m'/}
    output=${output//$'\e[m'/}
    summary=${output#*Overall Summary}
    read -r -a failed_names <<< "$failed"
    failed_count=${#failed_names[@]}
    check "exit $status, expected $expected" test "$status" -eq "$expected"
    check 'summary present' contains "$output" 'Overall Summary'
    check 'summary printed exactly once' test "${summary/Overall Summary/}" = "$summary"
    check 'all 13 tests counted' contains "$summary" 'Test Count: 13'
    check "passed count $((13-failed_count))" contains "$summary" "Tests Passed: $((13-failed_count))"
    if ((failed_count)); then
        check "failed count $failed_count" contains "$summary" "Tests Failed: $failed_count"
    else
        check 'no failures reported' contains "$summary" 'Failed Tests: 0'
    fi
    for name in "${suite[@]}"; do
        attempts=1 kept=0 calls=0 present=0
        if [[ " $failed " == *" $name "* ]]; then
            attempts=3 kept=1
            check "$name reported failed" contains "$summary" "$name.sh: FAILED"
        else
            check "$name reported passed" contains "$summary" "   $name.sh"
        fi
        [[ $scenario != retry || $name != query ]] || attempts=2
        [[ $mode != noCleanup && $CLEANUP_STATUS == 0 ]] || kept=1
        [[ ! -f "$sandbox/.calls/$name" ]] || read -r calls < "$sandbox/.calls/$name"
        check "$name attempts $calls, expected $attempts" test "$calls" -eq "$attempts"
        [[ ! -f "$sandbox/${name}_fixture/querySummary.txt" ]] || present=1
        check "$name artifact retained=$present, expected $kept" test "$present" -eq "$kept"
    done
    if [[ $mode == noCleanup ]]; then
        check 'cleanup skipped' test ! -f "$sandbox/.cleanup.calls"
    else
        check 'cleanup ran' test -f "$sandbox/.cleanup.calls"
    fi
    if [[ $CHILD_STATUS == 7 ]]; then
        check 'diagnostic mentions child exit 7' diagnostic '([Ee]xit|[Ss]tatus|[Cc]ode)[^0-9]*7([^0-9]|$)'
    elif [[ $CHILD_STATUS == 124 ]]; then
        check 'diagnostic mentions timeout and 2m' diagnostic '[Tt]imed out.*2m'
    fi
    if ((failures == before)); then
        printf 'PASS [%s]\n' "$label"
    else
        printf 'CASE [%s] %s failed assertions\n' "$label" "$((failures-before))"
    fi
}

for mode in default noCleanup; do
    run_case "all-pass-$mode" "$mode" pass 0 ''
    # The last query passes after three earlier failures: its status must not mask them.
    run_case "bad-counts-$mode" "$mode" counts 1 'batchLookup count discovery'
    run_case "child-7-$mode" "$mode" child 1 'batchLookup count' 0 'batchLookup count' 7
    run_case "child-124-$mode" "$mode" child 1 'discovery' 0 'discovery' 124
    run_case "retry-success-$mode" "$mode" retry 0 ''
done
run_case cleanup-failure default pass 23 '' 23
run_case cleanup-skipped noCleanup pass 0 '' 23
run_case test-and-cleanup-failure default counts 1 'batchLookup count discovery' 23
printf '\n%s cases, %s assertions, %s failures\n' "$cases" "$checks" "$failures"
((failures == 0))
