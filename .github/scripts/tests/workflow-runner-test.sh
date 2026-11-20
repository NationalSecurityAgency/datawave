#!/usr/bin/env bash

set -euo pipefail

readonly repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
readonly resolver='.github/scripts/resolve-runner.sh'
readonly workflows=(
    '.github/workflows/datawave-tests.yml'
    '.github/workflows/microservice-tests.yml'
    '.github/workflows/compose-tests.yml'
)

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

require_text() {
    local file="$1"
    local text="$2"

    grep -Fq -- "$text" "$repo_root/$file" || fail "$file is missing: $text"
}

for workflow in "${workflows[@]}"; do
    require_text "$workflow" 'name: Checkout runner configuration'
    require_text "$workflow" 'id: resolve_runner'
    require_text "$workflow" 'RUNNER_TYPE: ${{ vars.RUNNER_TYPE }}'
    require_text "$workflow" 'SUPPORTED_RUNNER_TYPES: ${{ vars.SUPPORTED_RUNNER_TYPES }}'
    require_text "$workflow" "run: $resolver"
    require_text "$workflow" 'runner: ${{ steps.resolve_runner.outputs.runner }}'
    require_text "$workflow" 'selection_source: ${{ steps.resolve_runner.outputs.selection_source }}'
    require_text "$workflow" 'fallback_reason: ${{ steps.resolve_runner.outputs.fallback_reason }}'
    require_text "$workflow" 'is_self_hosted: ${{ steps.resolve_runner.outputs.is_self_hosted }}'
    require_text "$workflow" 'runs-on: ${{ needs.configure-runner.outputs.runner }}'
    require_text "$workflow" 'needs.configure-runner.outputs.is_self_hosted'

    if grep -Eq 'outputs\.runner[^\n]*(==|!=)[^\n]*self-hosted' "$repo_root/$workflow"; then
        fail "$workflow still classifies self-hosted runners by the runner string"
    fi
done

require_text '.github/workflows/datawave-tests.yml' 'ref: ${{ github.sha }}'
require_text '.github/workflows/microservice-tests.yml' 'ref: ${{ github.sha }}'
require_text '.github/workflows/compose-tests.yml' 'ref: ${{ inputs.ref || github.sha }}'

printf 'PASS: %d workflow runner contracts\n' "${#workflows[@]}"
