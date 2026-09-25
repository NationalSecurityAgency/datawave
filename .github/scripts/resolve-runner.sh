#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly override_file="${RUNNER_OVERRIDE_FILE:-.github/runner-version}"
readonly repository_default="${RUNNER_TYPE-}"
readonly supported_runner_types="${SUPPORTED_RUNNER_TYPES-}"
readonly github_output="${GITHUB_OUTPUT:?GITHUB_OUTPUT must name the step output file}"

runner="$repository_default"
selection_source='repository-wide'
fallback_reason=''

trim_ascii_space_and_tab() {
    trimmed_value="$1"

    while [[ "$trimmed_value" == ' '* || "$trimmed_value" == $'\t'* ]]; do
        trimmed_value="${trimmed_value:1}"
    done
    while [[ "$trimmed_value" == *' ' || "$trimmed_value" == *$'\t' ]]; do
        trimmed_value="${trimmed_value:0:${#trimmed_value}-1}"
    done
}

is_supported() {
    local candidate="$1"
    local remaining="$supported_runner_types"
    local member
    local has_more

    while true; do
        if [[ "$remaining" == *,* ]]; then
            member="${remaining%%,*}"
            remaining="${remaining#*,}"
            has_more=true
        else
            member="$remaining"
            has_more=false
        fi

        trim_ascii_space_and_tab "$member"
        member="$trimmed_value"
        if [[ -n "$member" && "$candidate" == "$member" ]]; then
            return 0
        fi

        [[ "$has_more" == true ]] || break
    done

    return 1
}

if [[ -e "$override_file" || -L "$override_file" ]]; then
    candidate=''
    invalid_file=false

    if [[ ! -f "$override_file" || ! -r "$override_file" ]]; then
        invalid_file=true
    elif IFS= read -r -d '' candidate < "$override_file"; then
        # Bash read returns success only if it encountered the NUL delimiter.
        invalid_file=true
    else
        if [[ "$candidate" == *$'\r\n' ]]; then
            candidate="${candidate%$'\r\n'}"
        elif [[ "$candidate" == *$'\n' ]]; then
            candidate="${candidate%$'\n'}"
        fi

        if ! is_supported "$candidate"; then
            invalid_file=true
        fi
    fi

    if [[ "$invalid_file" == false ]]; then
        runner="$candidate"
        selection_source='branch-local'
    else
        fallback_reason='invalid branch-local override'
    fi
fi

if [[ "$selection_source" == 'branch-local' || "$runner" == 'self-hosted' || "$runner" =~ ^(self-hosted|runner-version-packer)-v[0-9]+$ ]]; then
    is_self_hosted=true
else
    is_self_hosted=false
fi

write_github_output() {
    local name="$1"
    local value="$2"
    local delimiter='__RUNNER_SELECTION_EOF__'

    if [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]]; then
        printf '%s=%s\n' "$name" "$value"
        return
    fi

    while [[ $'\n'"$value"$'\n' == *$'\n'"$delimiter"$'\n'* ]]; do
        delimiter="${delimiter}_"
    done
    printf '%s<<%s\n%s\n%s\n' "$name" "$delimiter" "$value" "$delimiter"
}

{
    write_github_output runner "$runner"
    write_github_output selection_source "$selection_source"
    write_github_output fallback_reason "$fallback_reason"
    write_github_output is_self_hosted "$is_self_hosted"
} >> "$github_output"

printf 'Runner selection: source=%s runner=%s fallback_reason=%s\n' \
    "$selection_source" "$runner" "${fallback_reason:-none}"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        printf '### Runner selection\n\n'
        printf '| Field | Value |\n| --- | --- |\n'
        printf '| Source | `%s` |\n' "$selection_source"
        printf '| Runner | `%s` |\n' "$runner"
        printf '| Fallback reason | `%s` |\n' "${fallback_reason:-none}"
    } >> "$GITHUB_STEP_SUMMARY"
fi

if [[ -n "$fallback_reason" ]]; then
    printf '::warning::%s; using repository-wide runner selection\n' "$fallback_reason"
fi
