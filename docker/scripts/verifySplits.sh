#!/bin/bash

# Regression guard for the shard table's split points.
#
# initialize-datawave.sh pre-splits datawave.shard from stack/shard-splits.txt so the
# sharded schema is spread over one tablet per shard row. Nothing fails loudly when
# that step is skipped, or when the file drifts away from the configured shards per
# day: queries return exactly the same answers, they just all land on one tablet
# server. So check it explicitly.
#
# Exits non-zero if the file is malformed, if it does not cover every shard of every
# day it names, or if any of its split points is missing from the table.

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
COMPOSE_DIR="$( dirname "${SCRIPT_DIR}" )"

SPLITS_FILE="${SPLITS_FILE:-${COMPOSE_DIR}/stack/shard-splits.txt}"
PROPERTIES_FILE="${PROPERTIES_FILE:-${COMPOSE_DIR}/../properties/compose.properties}"
SHARD_TABLE="${SHARD_TABLE:-datawave.shard}"
ACCUMULO_SERVICE="${ACCUMULO_SERVICE:-accumulo-manager}"
ACCUMULO_USER="${ACCUMULO_USER:-root}"
ACCUMULO_PASSWORD="${ACCUMULO_PASSWORD:-secret}"

FAILURES=0

fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

compose() {
    docker compose -f "${COMPOSE_DIR}/docker-compose.yml" --profile "${COMPOSE_PROFILE:-datawave-stack}" "$@"
}

if [ ! -s "${SPLITS_FILE}" ] ; then
    echo "FAIL: ${SPLITS_FILE} is missing or empty" >&2
    exit 1
fi

# Accumulo splits the table on every non-blank line of the file, so anything that is
# not a shard id - a comment, a stray blank-looking line - becomes a tablet boundary.
MALFORMED=$(grep -cv '^[0-9]\{8\}_[0-9]\{1,\}$' "${SPLITS_FILE}")
[ "${MALFORMED}" -eq 0 ] || fail "${MALFORMED} line(s) of ${SPLITS_FILE} are not a yyyyMMdd_num shard id"

LC_ALL=C sort -c -u "${SPLITS_FILE}" 2>/dev/null || fail "${SPLITS_FILE} is unsorted or holds duplicates"

# Ingest writes each day across shards 0 through numShardsPerDay-1, so a day covered
# by only some of its shards leaves the rest sharing a neighbour's tablet.
NUM_SHARDS=$(sed -n 's/^table\.shard\.numShardsPerDay=\([0-9]\{1,\}\)$/\1/p' "${PROPERTIES_FILE}" | tail -n 1)
if [ -z "${NUM_SHARDS}" ] ; then
    echo "WARN: no table.shard.numShardsPerDay in ${PROPERTIES_FILE}, not checking shard coverage" >&2
else
    # Compare the shard numbers as numbers. Comparing the joined strings instead
    # would sort 10 between 1 and 2 and call a complete file under-split.
    # @formatter:off
    UNCOVERED=$(awk -F_ -v n="${NUM_SHARDS}" '
            {
                days[$1] = 1
                seen[$1 "_" $2] = 1
                if ($2 + 0 >= n) { beyond[$1] = 1 }
            }
            END {
                for (day in days) {
                    if (day in beyond) { print day; continue }
                    for (i = 0; i < n; i++) {
                        if (!((day "_" i) in seen)) { print day; break }
                    }
                }
            }' "${SPLITS_FILE}" \
        | LC_ALL=C sort)
    # @formatter:on
    if [ -n "${UNCOVERED}" ] ; then
        fail "$(echo "${UNCOVERED}" | wc -l) day(s) of ${SPLITS_FILE} are not split into ${NUM_SHARDS} shards:"
        echo "${UNCOVERED}" | sed 's/^/    /' >&2
    fi
fi

# Keep the exit status of the shell itself. A table with no splits and a shell that
# never ran both produce no output, and reporting the second as the first sends the
# reader to the init script when the stack is simply down or the table is missing.
GETSPLITS_ERR=$(mktemp)
GETSPLITS_OUT=$(compose exec -T "${ACCUMULO_SERVICE}" \
    accumulo shell -u "${ACCUMULO_USER}" -p "${ACCUMULO_PASSWORD}" -e "getsplits -t ${SHARD_TABLE}" 2>"${GETSPLITS_ERR}")
GETSPLITS_STATUS=$?

if [ "${GETSPLITS_STATUS}" -ne 0 ] ; then
    echo "FAIL: could not read the splits of ${SHARD_TABLE} (exit ${GETSPLITS_STATUS})." >&2
    echo "The stack must be up and '${ACCUMULO_SERVICE}' running for this check. Accumulo said:" >&2
    sed 's/^/    /' "${GETSPLITS_ERR}" >&2
    rm -f "${GETSPLITS_ERR}"
    exit 1
fi
rm -f "${GETSPLITS_ERR}"

TABLE_SPLITS=$(echo "${GETSPLITS_OUT}" | tr -d '\r' | sed '/^[[:space:]]*$/d' | LC_ALL=C sort -u)

EXPECTED_SPLITS=$(LC_ALL=C sort -u "${SPLITS_FILE}")
MISSING=$(LC_ALL=C comm -23 <(echo "${EXPECTED_SPLITS}") <(echo "${TABLE_SPLITS}"))
# Extra splits are legitimate: Accumulo splits a tablet of its own accord once it
# outgrows table.split.threshold. Report them, but only the missing ones are a failure.
EXTRA=$(LC_ALL=C comm -13 <(echo "${EXPECTED_SPLITS}") <(echo "${TABLE_SPLITS}"))

echo "${SHARD_TABLE}: $(echo "${TABLE_SPLITS}" | grep -c .) split(s) on the table, $(grep -c . "${SPLITS_FILE}") expected from ${SPLITS_FILE}"

if [ -n "${MISSING}" ] ; then
    fail "$(echo "${MISSING}" | wc -l) split point(s) never made it onto ${SHARD_TABLE}:"
    echo "${MISSING}" | sed 's/^/    /' >&2
fi

if [ -n "${EXTRA}" ] ; then
    echo "NOTE: $(echo "${EXTRA}" | wc -l) split point(s) on ${SHARD_TABLE} are not in ${SPLITS_FILE}:"
    echo "${EXTRA}" | sed 's/^/    /'
fi

if [ "${FAILURES}" -ne 0 ] ; then
    exit 1
fi

echo "OK: ${SHARD_TABLE} carries every split point in ${SPLITS_FILE}"
