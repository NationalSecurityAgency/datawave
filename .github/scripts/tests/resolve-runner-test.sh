#!/usr/bin/env bash

set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly resolver="${script_dir}/../resolve-runner.sh"
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT

tests_run=0

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

output_value() {
    local output_file="$1"
    local requested_name="$2"
    local name value delimiter line first_line

    while IFS='=' read -r name value; do
        if [[ "$name" == "$requested_name" ]]; then
            printf '%s' "$value"
            return 0
        fi
        if [[ "$name" == "${requested_name}<<"* ]]; then
            delimiter="${name#*<<}"
            value=''
            first_line=true
            while IFS= read -r line; do
                [[ "$line" == "$delimiter" ]] && break
                if [[ "$first_line" == true ]]; then
                    value="$line"
                    first_line=false
                else
                    value+=$'\n'"$line"
                fi
            done
            printf '%s' "$value"
            return 0
        fi
    done < "$output_file"

    fail "missing output ${requested_name}"
}

assert_output() {
    local output_file="$1"
    local name="$2"
    local expected="$3"
    local actual

    actual="$(output_value "$output_file" "$name")"
    [[ "$actual" == "$expected" ]] || fail "${name}: expected <${expected}>, got <${actual}>"
}

run_case() {
    local name="$1"
    local file_state="$2"
    local contents="$3"
    local repository_default="$4"
    local supported="$5"
    local expected_runner="$6"
    local expected_source="$7"
    local expected_reason="$8"
    local expected_self_hosted="$9"
    local case_dir="${test_root}/${name}"
    local override_file="${case_dir}/runner-version"
    local output_file="${case_dir}/output"

    mkdir -p "$case_dir"
    if [[ "$file_state" == present ]]; then
        printf '%s' "$contents" > "$override_file"
    fi

    RUNNER_OVERRIDE_FILE="$override_file" \
        RUNNER_TYPE="$repository_default" \
        SUPPORTED_RUNNER_TYPES="$supported" \
        GITHUB_OUTPUT="$output_file" \
        "$resolver" || fail "${name}: resolver exited non-zero"

    assert_output "$output_file" runner "$expected_runner"
    assert_output "$output_file" selection_source "$expected_source"
    assert_output "$output_file" fallback_reason "$expected_reason"
    assert_output "$output_file" is_self_hosted "$expected_self_hosted"
    tests_run=$((tests_run + 1))
}

run_case absent absent '' ubuntu-latest 'self-hosted-v1' ubuntu-latest repository-wide '' false
run_case valid-allowlisted present 'self-hosted-v2' ubuntu-latest $' self-hosted-v1,\tself-hosted-v2,, ' self-hosted-v2 branch-local '' true
run_case valid-terminal-lf present $'self-hosted-v2\n' ubuntu-latest 'self-hosted-v2' self-hosted-v2 branch-local '' true
run_case valid-terminal-crlf present $'self-hosted-v2\r\n' ubuntu-latest 'self-hosted-v2' self-hosted-v2 branch-local '' true
run_case valid-packer present 'runner-version-packer-v1' ubuntu-latest 'runner-version-packer-v1' runner-version-packer-v1 branch-local '' true
run_case valid-custom-label present 'linux-gpu-large' ubuntu-latest 'linux-gpu-large' linux-gpu-large branch-local '' true
run_case empty present '' self-hosted 'self-hosted-v2' self-hosted repository-wide 'invalid branch-local override' true
run_case whitespace-only present $' \t\n' ubuntu-latest 'self-hosted-v2' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case padded present $' self-hosted-v2\n' ubuntu-latest 'self-hosted-v2' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case multiline present $'self-hosted-v2\nself-hosted-v3\n' ubuntu-latest 'self-hosted-v2,self-hosted-v3' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case unsupported present 'self-hosted-vx' ubuntu-latest 'self-hosted-v2' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case valid-unlisted present 'self-hosted-v3' ubuntu-latest 'self-hosted-v2' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case empty-allowlist present 'self-hosted-v2' ubuntu-latest '' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case case-sensitive present 'self-hosted-v2' ubuntu-latest 'SELF-HOSTED-V2' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case allowlist-newline-is-not-trimmed present 'self-hosted-v2' ubuntu-latest $'self-hosted-v2\n' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case duplicate-allowlist present 'self-hosted-v2' ubuntu-latest 'self-hosted-v2,self-hosted-v2' self-hosted-v2 branch-local '' true
run_case lone-terminal-cr present $'self-hosted-v2\r' ubuntu-latest 'self-hosted-v2' ubuntu-latest repository-wide 'invalid branch-local override' false
run_case multiline-repository-default present 'not-valid' $'custom\nrunner' 'self-hosted-v2' $'custom\nrunner' repository-wide 'invalid branch-local override' false

nul_case_dir="${test_root}/nul"
mkdir -p "$nul_case_dir"
printf 'self-hosted-v2\0ignored' > "${nul_case_dir}/runner-version"
RUNNER_OVERRIDE_FILE="${nul_case_dir}/runner-version" \
    RUNNER_TYPE='ubuntu-latest' \
    SUPPORTED_RUNNER_TYPES='self-hosted-v2' \
    GITHUB_OUTPUT="${nul_case_dir}/output" \
    "$resolver" || fail 'nul: resolver exited non-zero'
assert_output "${nul_case_dir}/output" runner ubuntu-latest
assert_output "${nul_case_dir}/output" selection_source repository-wide
assert_output "${nul_case_dir}/output" fallback_reason 'invalid branch-local override'
assert_output "${nul_case_dir}/output" is_self_hosted false
tests_run=$((tests_run + 1))

printf 'PASS: %d runner resolver cases\n' "$tests_run"
