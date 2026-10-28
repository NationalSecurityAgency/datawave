package datawave.webservice.config;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class QueryMetricIngestProperties {
    private String zookeepers;
    private String instanceName;
    private String username;
    private String password;
    private String metadataTableName;
    private String shardTableName;
    private String indexTableName;
    private String reverseIndexTableName;
    private int numShards;
    private List<String> indexedFields;
    private List<String> reverseIndexedFields;
    private List<String> numericFields;
    private long recordWriterMaxMemory = 10000000;
    private long recordWriterMaxLatency = 60000;
    private int recordWriterNumThreads = 4;
    private int fieldLengthThreshold = 4049;
    private String queryMetricsIngestPolicyEnforcerClass = "datawave.query.metrics.ingest.policy.DefaultIngestPolicyEnforcer";
    private String queryVisibility;
    private String defaultMetricVisibility;

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }

    public String getShardTableName() {
        return shardTableName;
    }

    public void setShardTableName(String shardTableName) {
        this.shardTableName = shardTableName;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }

    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }

    public int getNumShards() {
        return numShards;
    }

    public void setNumShards(int numShards) {
        this.numShards = numShards;
    }

    public List<String> getIndexedFields() {
        return indexedFields;
    }

    public void setIndexedFields(List<String> indexedFields) {
        this.indexedFields = indexedFields;
    }

    public List<String> getReverseIndexedFields() {
        return reverseIndexedFields;
    }

    public void setReverseIndexedFields(List<String> reverseIndexedFields) {
        this.reverseIndexedFields = reverseIndexedFields;
    }

    public List<String> getNumericFields() {
        return numericFields;
    }

    public void setNumericFields(List<String> numericFields) {
        this.numericFields = numericFields;
    }

    public long getRecordWriterMaxMemory() {
        return recordWriterMaxMemory;
    }

    public void setRecordWriterMaxMemory(long recordWriterMaxMemory) {
        this.recordWriterMaxMemory = recordWriterMaxMemory;
    }

    public int getRecordWriterNumThreads() {
        return recordWriterNumThreads;
    }

    public void setRecordWriterNumThreads(int recordWriterNumThreads) {
        this.recordWriterNumThreads = recordWriterNumThreads;
    }

    public long getRecordWriterMaxLatency() {
        return recordWriterMaxLatency;
    }

    public void setRecordWriterMaxLatency(long recordWriterMaxLatency) {
        this.recordWriterMaxLatency = recordWriterMaxLatency;
    }

    public String getQueryMetricsIngestPolicyEnforcerClass() {
        return queryMetricsIngestPolicyEnforcerClass;
    }

    public void setQueryMetricsIngestPolicyEnforcerClass(String queryMetricsIngestPolicyEnforcerClass) {
        this.queryMetricsIngestPolicyEnforcerClass = queryMetricsIngestPolicyEnforcerClass;
    }

    public int getFieldLengthThreshold() {
        return fieldLengthThreshold;
    }

    public void setFieldLengthThreshold(int fieldLengthThreshold) {
        this.fieldLengthThreshold = fieldLengthThreshold;
    }

    public String getQueryVisibility() {
        return queryVisibility;
    }

    public void setQueryVisibility(String queryVisibility) {
        this.queryVisibility = queryVisibility;
    }

    public String getDefaultMetricVisibility() {
        return defaultMetricVisibility;
    }

    public void setDefaultMetricVisibility(String defaultMetricVisibility) {
        this.defaultMetricVisibility = defaultMetricVisibility;
    }

    public Configuration getQueryMetricIngestConfiguration() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("AccumuloRecordWriter.zooKeepers", this.getZookeepers());
        conf.set("AccumuloRecordWriter.instanceName", this.getInstanceName());
        conf.set("AccumuloRecordWriter.username", this.getUsername());
        conf.set("AccumuloRecordWriter.password", this.getPassword());
        conf.setBoolean("AccumuloRecordWriter.createtables", true);
        conf.set(this.getShardTableName() + ".table.config.class", "datawave.ingest.table.config.ShardTableConfigHelper");
        conf.set(this.getIndexTableName() + ".table.config.class", "datawave.ingest.table.config.ShardTableConfigHelper");
        conf.set(this.getReverseIndexTableName() + ".table.config.class", "datawave.ingest.table.config.ShardTableConfigHelper");
        conf.set(this.getMetadataTableName() + ".table.config.class", "datawave.ingest.table.config.MetadataTableConfigHelper");
        conf.set("metadata.table.name", this.getMetadataTableName());
        conf.setBoolean("metadata.term.frequency.enabled", true);
        conf.setInt("num.shards", this.getNumShards());
        conf.set("sharded.table.names", this.getShardTableName());
        conf.set("shard.table.name", this.getShardTableName());
        conf.set("shard.table.locality.groups", "fullcontent:d,termfrequency:tf");
        conf.set("shard.global.index.table.name", this.getIndexTableName());
        conf.set("shard.global.rindex.table.name", this.getReverseIndexTableName());
        conf.set("data.name", "querymetrics");
        conf.set("querymetrics.ingest.fatal.errors", "UUID_MISSING");
        conf.set("querymetrics.ingest.helper.class", "datawave.query.metrics.ContentQueryMetricsIngestHelper");
        conf.set("querymetrics.data.category.date", "CREATE_DATE");
        conf.set("querymetrics.data.category.date.format", "yyyyMMdd HHmmss.S");
        conf.set("querymetrics.data.separator", ",");
        conf.set("querymetrics.data.category.uuid.fields", "QUERY_ID");
        conf.set("querymetrics.data.header", "none");
        conf.setInt("querymetrics.data.field.length.threshold", this.getFieldLengthThreshold());
        conf.set("querymetrics.data.category.index", String.join(",", this.getIndexedFields()));
        conf.set("querymetrics.data.category.index.reverse", String.join(",", this.getReverseIndexedFields()));
        conf.set("querymetrics.data.category.token.fieldname.designator", "");
        conf.set("querymetrics.data.default.type.class", "datawave.data.type.LcNoDiacriticsType");
        this.getNumericFields().forEach(field -> conf.set("querymetrics." + field + ".data.field.type.class", "datawave.data.type.NumberType"));
        conf.setLong("AccumuloRecordWriter.maxmemory", this.getRecordWriterMaxMemory());
        conf.setLong("AccumuloRecordWriter.maxlatency", this.getRecordWriterMaxLatency());
        conf.setInt("AccumuloRecordWriter.writethreads", this.getRecordWriterNumThreads());
        conf.set("querymetrics.ingest.policy.enforcer.class", this.getQueryMetricsIngestPolicyEnforcerClass());
        return conf;
    }
}
