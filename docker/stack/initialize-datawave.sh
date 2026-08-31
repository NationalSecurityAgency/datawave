#!/usr/bin/env bash
set -euo pipefail

readonly INGEST_HOME=/opt/datawave-ingest/current
readonly HDFS_INGEST_DIR=/datawave/ingest
readonly FIXTURES=/opt/datawave-test-data
readonly SHARD_SPLITS=/stack/shard-splits.txt
readonly NUM_SHARDS_FLOOR_DATE=19000101

until accumulo shell -u root -p secret -e info >/dev/null 2>&1; do
    echo "Waiting for Accumulo..."
    sleep 2
done

# The quickstart bootstrap created these as the HDFS superuser before running
# DataWave as its service account. Preserve that ownership transition here.
HADOOP_USER_NAME=hdfs hdfs dfs -mkdir -p \
    /accumulo /tmp/hadoop-yarn/staging/history "${HDFS_INGEST_DIR}"
HADOOP_USER_NAME=hdfs hdfs dfs -chmod -R 777 /tmp /datawave
accumulo shell -u root -p secret -e \
    "setauths -u root -s JBOSS_ADMIN,DW_ADMIN,AUTH_USER,BAR,FOO,PRIVATE,PUBLIC,PUB,PVT,DEF,A,B,C,D,E,F,G,H,I,DW_USER,DW_SERV"
accumulo shell -u root -p secret -e "createnamespace datawave" 2>/dev/null || true
accumulo shell -u root -p secret -e "createtable datawave.queryMetrics_m" 2>/dev/null || true
accumulo shell -u root -p secret -e "createtable datawave.queryMetrics_s" 2>/dev/null || true

mkdir -p /srv/logs/ingest /srv/data/datawave/flags /var/run/datawave

"${INGEST_HOME}/bin/ingest/create-all-tables.sh"

# Otherwise the whole sharded schema lives in one tablet.
accumulo shell -u root -p secret -e "addsplits -t datawave.shard -sf ${SHARD_SPLITS}"

# The query side expands a day into shards from this entry, not from num.shards.
num_shards=$(cut -d_ -f2 "${SHARD_SPLITS}" | sort -u | wc -l)
accumulo shell -u root -p secret -e "insert num_shards ns ${NUM_SHARDS_FLOOR_DATE}_${num_shards} '' -t datawave.metadata"

"${INGEST_HOME}/bin/ingest/load-job-cache.sh"

run_ingest() {
    local source_path="$1"
    local hdfs_path="$2"
    local input_format="$3"
    local data_type="$4"

    hdfs dfs -mkdir -p "$(dirname "${hdfs_path}")"
    hdfs dfs -copyFromLocal -f "${source_path}" "${hdfs_path}"
    set +e
    "${INGEST_HOME}/bin/ingest/live-ingest.sh" "${hdfs_path}" 10 \
        -inputFormat "${input_format}" -data.name.override="${data_type}" \
        -mapreduce.map.memory.mb=1536 -mapreduce.reduce.memory.mb=1536
    local status=$?
    set -e

    # Quickstart continues after status 251 because its example annotation and
    # CSV fixtures deliberately exercise DataWave's error-table path.
    if [[ ${status} -ne 0 && ${status} -ne 251 ]]; then
        return "${status}"
    fi
}

# The old quickstart ran these five files as separate Wikipedia jobs. A single
# job over the same files produces the same rows and cuts four YARN startups.
hdfs dfs -mkdir -p "${HDFS_INGEST_DIR}/wikipedia"
hdfs dfs -copyFromLocal -f \
    "${FIXTURES}/wikipedia/enwiki-20130305-pages-articles-brief.xml" \
    "${FIXTURES}/wikipedia/enwiki-20250519-pages-articles-medium.xml.gz" \
    "${FIXTURES}/wikipedia/dewiki-20250520-pages-articles-brief.xml" \
    "${FIXTURES}/wikipedia/eswiki-20250520-pages-articles-brief.xml" \
    "${FIXTURES}/wikipedia/frwiki-20250520-pages-articles-brief.xml" \
    "${HDFS_INGEST_DIR}/wikipedia/"
"${INGEST_HOME}/bin/ingest/live-ingest.sh" "${HDFS_INGEST_DIR}/wikipedia" 10 \
    -inputFormat datawave.ingest.wikipedia.WikipediaEventInputFormat \
    -data.name.override=wikipedia \
    -mapreduce.map.memory.mb=1536 -mapreduce.reduce.memory.mb=1536
run_ingest "${FIXTURES}/annotation/doubleAnnotation.json" "${HDFS_INGEST_DIR}/doubleAnnotation.json" \
    datawave.ingest.annotation.mapreduce.input.SimpleAnnotationInputFormat annotation
run_ingest "${FIXTURES}/json/tvmaze-api.json" "${HDFS_INGEST_DIR}/tvmaze-api.json" \
    datawave.ingest.json.mr.input.JsonInputFormat myjson
run_ingest "${FIXTURES}/csv/my.csv" "${HDFS_INGEST_DIR}/my.csv" \
    datawave.ingest.csv.mr.input.CSVFileInputFormat mycsv

touch /tmp/datawave-ready
exec tail -f /dev/null
