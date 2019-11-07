package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.TestContextWriter;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import datawave.util.TableName;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricsServiceTest {
    
    private DateFormat df = new SimpleDateFormat("yyyyMMdd");
    
    private String metricsTable = "ingestMetrics";
    private String metricName = Metric.KV_PER_TABLE.toString();
    private String expectedRow = df.format(new Date()) + "_0";
    
    private TaskInputOutputContext<LongWritable,Text,BulkIngestKey,Value> context;
    private TestContextWriter<BulkIngestKey,Value> contextWriter;
    private Configuration conf;
    private MetricsService<BulkIngestKey,Value> service;
    
    private Map<String,String> labels;
    
    @Before
    public void setUp() {
        conf = MetricsTestData.loadDefaultTestConfig();
        
        context = new StandaloneTaskAttemptContext<>(conf, null);
        contextWriter = new TestContextWriter<>();
        
        service = new MetricsService<>(contextWriter, context);
        
        labels = new HashMap<>();
        labels.put("table", TableName.SHARD);
        labels.put("handler", DummyDataTypeHandler.class.getName());
    }
    
    @Test
    public void shouldGenerateMetricsWhenConfigured() throws Exception {
        // NOTE: user is not a configured field to be saved
        Multimap<String,NormalizedContentInterface> fields = MetricsTestData.createFields("table", TableName.SHARD, "fileExtension", "gz", "user", "tommy");
        
        labels.put("dataType", "flow1");
        service.collect(Metric.KV_PER_TABLE, labels, fields, 1L);
        service.close();
        
        Multimap<BulkIngestKey,Value> pairs = contextWriter.getWritten();
        
        assertEquals(2, pairs.size());
        assertTrue(pairs.containsEntry(new BulkIngestKey(new Text(metricsTable), new Key(expectedRow, metricName, "table\u0000shard\u0000shard")), new Value(
                        "1".getBytes())));
        assertTrue(pairs.containsEntry(new BulkIngestKey(new Text(metricsTable), new Key(expectedRow, metricName, "fileExtension\u0000gz\u0000shard")),
                        new Value("1".getBytes())));
    }
    
    @Test
    public void shouldDropMetricsWhenNotConfigured() throws Exception {
        Multimap<String,NormalizedContentInterface> fields = MetricsTestData.createFields("table", TableName.SHARD, "fileExtension", "gz");
        
        // dataType 'flow2' is not enabled in the conf file
        labels.put("dataType", "flow2");
        service.collect(Metric.KV_PER_TABLE, labels, fields, 1L);
        service.close();
        
        Multimap<BulkIngestKey,Value> pairs = contextWriter.getWritten();
        
        assertTrue("Expected no metrics to be written due to missing dataType", pairs.isEmpty());
    }
    
    @Test
    public void shouldStillIncludeMetricsWithOnlyASubsetOfEnabledLabels() throws Exception {
        Multimap<String,NormalizedContentInterface> fields = MetricsTestData.createFields("table", TableName.SHARD);
        
        // dataType is not set
        service.collect(Metric.KV_PER_TABLE, labels, fields, 1L);
        service.close();
        
        Multimap<BulkIngestKey,Value> pairs = contextWriter.getWritten();
        
        assertEquals(1, pairs.size());
        assertTrue(pairs.containsEntry(new BulkIngestKey(new Text(metricsTable), new Key(expectedRow, metricName, "table\u0000shard\u0000shard")), new Value(
                        "1".getBytes())));
    }
    
    @Test
    public void shouldSupportWildcardLabels() throws Exception {
        // Enable all dataTypes using the wildcard
        Configuration conf = new Configuration();
        
        conf.setBoolean(MetricsConfiguration.METRICS_ENABLED_CONFIG, true);
        conf.setInt(MetricsConfiguration.NUM_SHARDS_CONFIG, 2);
        conf.set(MetricsConfiguration.METRICS_TABLE_CONFIG, metricsTable);
        conf.set(MetricsConfiguration.ENABLED_LABELS_CONFIG, "table=shard,table=shardIndex,dataType=*,handler=" + DummyDataTypeHandler.class.getName());
        conf.set(MetricsConfiguration.FIELDS_CONFIG, "table,fileExtension");
        conf.set(MetricsConfiguration.RECEIVERS_CONFIG, TestKeyValueCountMetricsReceiver.class.getName());
        
        context = new StandaloneTaskAttemptContext<>(conf, null);
        service = new MetricsService(contextWriter, context);
        
        // metric fields
        Multimap<String,NormalizedContentInterface> fields = MetricsTestData.createFields("table", TableName.SHARD, "fileExtension", "gz");
        
        labels.put("dataType", "flow2");
        service.collect(Metric.KV_PER_TABLE, labels, fields, 1L);
        service.close();
        
        Multimap<BulkIngestKey,Value> pairs = contextWriter.getWritten();
        
        assertEquals(2, pairs.size());
        assertTrue(pairs.containsEntry(new BulkIngestKey(new Text(metricsTable), new Key(expectedRow, metricName, "table\u0000shard\u0000shard")), new Value(
                        "1".getBytes())));
        assertTrue(pairs.containsEntry(new BulkIngestKey(new Text(metricsTable), new Key(expectedRow, metricName, "fileExtension\u0000gz\u0000shard")),
                        new Value("1".getBytes())));
    }
}
