#!/bin/bash
#
# Verifies the ingest scripts' classpath against the distribution staged in target/archive, failing
# if a jar variable used by ingest-libs.sh resolves to nothing or a missing file, or if a main class
# handed to the accumulo launcher is not resolvable on that classpath.
#
# The scripts glob for their jars at runtime, so a renamed or dropped jar leaves an empty variable
# and the deployment dies later with a ClassNotFoundException.
#
# This proves the assembly is self-consistent, not that the classpath is complete for everything a
# main class goes on to touch; the JVM resolves those lazily and optional dependencies make that
# impossible to assert.

set -u

ARCHIVE="${1:?usage: verify-ingest-classpath.sh <path to target/archive>}"

if [[ ! -d "${ARCHIVE}/bin/ingest" || ! -d "${ARCHIVE}/lib" ]]; then
    echo "[classpath-check] SKIPPED: ${ARCHIVE} has no staged bin/ and lib/ layout"
    exit 0
fi

cd "${ARCHIVE}/bin/ingest" || exit 1

# findJars.sh reads these from the deployed environment. Default them to the staged lib directory,
# which carries accumulo and zookeeper as ordinary Maven dependencies, but let a real environment
# win so this script can be run unchanged against an installed ingest home as a post-deploy check.
export WAREHOUSE_ACCUMULO_LIB="${WAREHOUSE_ACCUMULO_LIB:-${ARCHIVE}/lib}"
export ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-${ARCHIVE}/lib}"

FIND_JARS_ERRORS=$(mktemp)
trap 'rm -f "${FIND_JARS_ERRORS}"' EXIT

# Collect the variables ingest-libs.sh actually puts on the classpath. Driving the check from the
# consumer rather than from findJars.sh is deliberate: a variable that ingest-libs.sh references
# but findJars.sh never assigns is exactly the defect we are looking for, and iterating over
# findJars.sh's assignments would step right over it.
CLASSPATH_VARS=$(
    grep -E '^CLASSPATH=' ./ingest-libs.sh \
        | grep -oE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' \
        | tr -d '${}' | sort -u | grep -vx 'CLASSPATH'
)

# shellcheck disable=SC1091
source ./findJars.sh 2>"${FIND_JARS_ERRORS}"

# Rebuild the classpath exactly as ingest-libs.sh does. We cannot source ingest-libs.sh directly
# because it pulls in ingest-env.sh, which refuses to run outside a configured ingest host.
# 'set +u' here is not laziness: ingest-libs.sh runs without it on a real cluster, so an unset
# variable expands to an empty entry instead of failing. Reproducing that faithfully is the point
# -- the empty entry is then reported by the per-variable check below.
CLASSPATH=""
set +u
while IFS= read -r assignment; do
    eval "${assignment}"
done < <(grep -E '^CLASSPATH=' ./ingest-libs.sh)
set -u

FAILURES=0

fail() {
    echo "[classpath-check] FAIL: $*"
    FAILURES=$((FAILURES + 1))
}

#
# 1. Every jar variable that ingest-libs.sh puts on the classpath must resolve to a real file.
#
for var in ${CLASSPATH_VARS}; do
    # Written by create-edgekey-version-cache.sh on a running cluster, so it is legitimately
    # absent from a freshly staged archive.
    [[ "${var}" == "EDGE_KEY_VERSION_CACHE_FILE" ]] && continue

    # Tested with -v rather than a defaulted expansion: under 'set -u' bash still aborts on an
    # indirect reference to an unset name, and an unset name is a case we want to report.
    if [[ -v "${var}" ]]; then
        value="${!var}"
    else
        value=""
    fi

    if [[ -z "${value}" ]]; then
        fail "\$${var} is on the classpath but resolved to nothing (findJars.sh assigns no value, or no jar in lib/ matches)"
        continue
    fi
    # A few variables hold several colon-separated jars.
    IFS=':' read -ra entries <<< "${value}"
    for entry in "${entries[@]}"; do
        if [[ -z "${entry}" ]]; then
            fail "\$${var} contains an empty classpath entry"
        elif [[ ! -e "${entry}" ]]; then
            fail "\$${var} points at ${entry}, which does not exist"
        fi
    done
done

if [[ -s "${FIND_JARS_ERRORS}" ]]; then
    echo "[classpath-check] findJars.sh reported errors while resolving jars:"
    sed 's/^/    /' "${FIND_JARS_ERRORS}"
    FAILURES=$((FAILURES + 1))
fi

#
# 2. Every main class handed to the accumulo launcher must be resolvable on that classpath.
#
MAIN_CLASSES=$(
    cat ./*.sh ../util/*.sh ../metrics/*.sh ../system/*.sh 2>/dev/null \
        | tr '\n' ' ' | sed 's/\\ */ /g' \
        | grep -oE 'accumulo +datawave\.[A-Za-z0-9_.]*' \
        | awk '{print $2}' | sort -u
)

if [[ -z "${MAIN_CLASSES}" ]]; then
    fail "found no accumulo launcher invocations to check -- has the script layout changed?"
fi

for class in ${MAIN_CLASSES}; do
    if javap -cp "${CLASSPATH}" "${class}" > /dev/null 2>&1; then
        echo "[classpath-check] ok: ${class}"
    else
        fail "${class} is launched by an ingest script but is not on the classpath ingest-libs.sh builds"
    fi
done

if [[ ${FAILURES} -gt 0 ]]; then
    echo "[classpath-check] ${FAILURES} problem(s) found in the staged ingest classpath"
    exit 1
fi

echo "[classpath-check] staged ingest classpath verified: $(echo "${CLASSPATH}" | tr ':' '\n' | grep -c .) entries, $(echo "${MAIN_CLASSES}" | wc -w) main class(es)"
