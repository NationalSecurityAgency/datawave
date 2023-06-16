package datawave.metrics.config;

import datawave.util.TableName;

import java.util.concurrent.TimeUnit;

public class MetricsConfig {
    public static final String MTX = "metrics.";

    public static final String ZOOKEEPERS = MTX + "zookeepers";

    public static final String USER = MTX + "user";

    public static final String PASS = MTX + "password";

    public static final String DEFAULT_VISIBILITY = MTX + "defaultvis";

    public static final String INSTANCE = MTX + "instance";

    public static final String WAR_FILE = MTX + "war";

    public static final String CONF_FILE = MTX + "conf";

    public static final String START = MTX + "start";

    public static final String END = MTX + "end";

    /* Specifies the type of metrics job being run- ingest | loader | analytic */
    public static final String TYPE = MTX + "type";

    public static final String INPUT_DIRECTORY = MTX + "input";

    public static final String INGEST_TABLE = "metrics.tables.ingest";
    public static final String DEFAULT_INGEST_TABLE = "IngestMetrics";

    public static final String FLAGMAKER_TABLE = "metrics.tables.flagmaker";
    public static final String DEFAULT_FLAGMAKER_TABLE = "FlagMakerMetrics";

    public static final String LOADER_TABLE = "metrics.tables.loader";
    public static final String DEFAULT_LOADER_TABLE = "LoaderMetrics";

    public static final String METRICS_TABLE = "metrics.tables.metrics";
    public static final String DEFAULT_METRICS_TABLE = "DatawaveMetrics";

    public static final String METRICS_SUMMARY_TABLE = "metrics.tables.metrics-summary";
    public static final String DEFAULT_METRICS_SUMMARY_TABLE = "DatawaveDailyMetrics";
    public static final String METRICS_HOURLY_SUMMARY_TABLE = "metrics.tables.metrics-hourly-summary";
    public static final String DEFAULT_HOURLY_METRICS_SUMMARY_TABLE = "DatawaveHourlyMetrics";

    public static final String ERROR_SHARD_TABLE = "metrics.summary.tables.error-shard";
    public static final String DEFAULT_ERROR_SHARD_TABLE = TableName.ERROR_SHARD;

    public static final String BAD_SELECTOR_TABLE = "metrics.summary.tables.bad-selector";
    public static final String DEFAULT_BAD_SELECTOR_TABLE = "BadSelector";

    public static final String HOURLY_SUMMARY = "metrics-summary.table.name";
    public static final String DEFAULT_HOURLY_SUMMARY = "metricsSummary";

    public static final String QUERY_METRICS_EVENT_TABLE = "metrics.tables.query-metrics-event";
    public static final String DEFAULT_QUERY_METRICS_EVENT_TABLE = "QueryMetrics_e";

    public static final String RAW_FILE_INDEX_TABLE = "metrics.tables.raw-file";
    public static final String DEFAULT_RAW_FILE_INDEX_TABLE = "FalloutErrorsIndex";

    public static final String TIME_BOUNDARY = "metrics.analytic.boundary";
    public static final long DEFAULT_TIME_BOUNDARY = TimeUnit.MINUTES.toMillis(30);

    public static final String ERRORS_TABLE = "metrics.tables.error";

    public static final String DEFAULT_ERRORS_TABLE = "processingErrors";

    public static final String WAREHOUSE_HADOOP_PATH = MTX + "warehouse.hadoop.path";
    public static final String WAREHOUSE_INSTANCE = "metrics.warehouse.instance";
    public static final String WAREHOUSE_USERNAME = "metrics.warehouse.username";
    public static final String WAREHOUSE_PASSWORD = "metrics.warehouse.password";
    public static final String WAREHOUSE_ZOOKEEPERS = "metrics.warehouse.zookeepers";

    public static final String FILE_GRAPH_TABLE = MTX + "tables.filegraph";
    public static final String DEFAULT_FILE_GRAPH_TABLE = "FileLatencies";

    public static final String LIVE_INGEST_THRESHOLD = MTX + "ingest.live.threshold";
    public static final String BULK_INGEST_THRESHOLD = MTX + "ingest.bulk.threshold";

    public static final String USE_HOURLY_PRECISION = "metrics.use.hourly.precision";
    public static final String DEFAULT_USE_HOURLY_PRECISION = "false";

}
