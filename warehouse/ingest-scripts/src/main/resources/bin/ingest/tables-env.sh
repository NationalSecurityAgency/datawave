#!/bin/bash

# To guard against invalid table names and bash variable expansion errors,
# ensure that any value here intended to be interpolated from Maven properties
# has a corresponding 'assert-properties' plugin assertion activated for the
# given property name

NUM_SHARDS=${table.shard.numShardsPerDay}
NUM_DATE_INDEX_SHARDS=${table.dateIndex.numShardsPerDay}

SHARD_TABLE_NAME="${table.name.shard}"
SHARD_TABLE_NAME="${SHARD_TABLE_NAME:-shard}"
SHARD_STATS_TABLE_NAME="${table.name.shardStats}"
SHARD_STATS_TABLE_NAME="${SHARD_STATS_TABLE_NAME:-shardStats}"
SHARD_INDEX_TABLE_NAME="${table.name.shardIndex}"
SHARD_INDEX_TABLE_NAME="${SHARD_INDEX_TABLE_NAME:-shardIndex}"
SHARD_REVERSE_INDEX_TABLE_NAME="${table.name.shardReverseIndex}"
SHARD_REVERSE_INDEX_TABLE_NAME="${SHARD_REVERSE_INDEX_TABLE_NAME:-shardReverseIndex}"

METADATA_TABLE_NAME="${table.name.metadata}"
METADATA_TABLE_NAME="${METADATA_TABLE_NAME:-datawaveMetadata}"

EDGE_TABLE_NAME="${table.name.edge}"
EDGE_TABLE_NAME="${EDGE_TABLE_NAME:-edge}"

ERROR_METADATA_TABLE_NAME="${table.name.errors.metadata}"
ERROR_METADATA_TABLE_NAME="${ERROR_METADATA_TABLE_NAME:-ingestErrors_m}"
ERROR_SHARD_TABLE_NAME="${table.name.errors.shard}"
ERROR_SHARD_TABLE_NAME="${ERROR_SHARD_TABLE_NAME:-ingestErrors_s}"
ERROR_SHARD_INDEX_TABLE_NAME="${table.name.errors.shardIndex}"
ERROR_SHARD_INDEX_TABLE_NAME="${ERROR_SHARD_INDEX_TABLE_NAME:-ingestErrors_i}"
ERROR_SHARD_REVERSE_INDEX_TABLE_NAME="${table.name.errors.shardReverseIndex}"
ERROR_SHARD_REVERSE_INDEX_TABLE_NAME="${ERROR_SHARD_REVERSE_INDEX_TABLE_NAME:-ingestErrors_r}"

QUERYMETRICS_METADATA_TABLE_NAME="${table.name.queryMetrics.metadata}"
QUERYMETRICS_METADATA_TABLE_NAME="${QUERYMETRICS_METADATA_TABLE_NAME:-queryMetrics_m}"
QUERYMETRICS_SHARD_TABLE_NAME="${table.name.queryMetrics.shard}"
QUERYMETRICS_SHARD_TABLE_NAME="${QUERYMETRICS_SHARD_TABLE_NAME:-queryMetrics_s}"
QUERYMETRICS_SHARD_INDEX_TABLE_NAME="${table.name.queryMetrics.shardIndex}"
QUERYMETRICS_SHARD_INDEX_TABLE_NAME="${QUERYMETRICS_SHARD_INDEX_TABLE_NAME:-queryMetrics_i}"
QUERYMETRICS_SHARD_REVERSE_INDEX_TABLE_NAME="${table.name.queryMetrics.shardReverseIndex}"
QUERYMETRICS_SHARD_REVERSE_INDEX_TABLE_NAME="${QUERYMETRICS_SHARD_REVERSE_INDEX_TABLE_NAME:-queryMetrics_r}"
QUERYMETRICS_DATE_INDEX_TABLE_NAME="${table.name.queryMetrics.dateIndex}"
QUERYMETRICS_DATE_INDEX_TABLE_NAME="${QUERYMETRICS_DATE_INDEX_TABLE_NAME:-queryMetrics_di}"
