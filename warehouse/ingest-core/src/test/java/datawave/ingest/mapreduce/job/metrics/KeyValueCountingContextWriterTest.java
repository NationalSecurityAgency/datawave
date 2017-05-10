package datawave.ingest.mapreduce.job.metrics;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.TestContextWriter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static datawave.ingest.mapreduce.job.metrics.MetricsTestData.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyValueCountingContextWriterTest {
    
    private TaskInputOutputContext<LongWritable,Text,BulkIngestKey,Value> context;
    private TestContextWriter<BulkIngestKey,Value> contextWriter;
    private KeyValueCountingContextWriter<BulkIngestKey,Value> kvContextWriter;
    
    private RawRecordContainer fileEvent;
    private Multimap<String,NormalizedContentInterface> fileFields;
    
    private DataTypeHandler<BulkIngestKey> handler = new DummyDataTypeHandler<>();
    
    @Before
    public void setUp() {
        
        Type fileType = new Type("file", null, null, null, 10, null);
        fileEvent = createEvent(fileType);
        fileFields = createFields("extension", "gz", "extension", "tar", "lastModified", "2016-01-01");
        
        Configuration conf = new Configuration();
        
        conf.setBoolean(MetricsConfiguration.METRICS_ENABLED_CONFIG, true);
        conf.setInt(MetricsConfiguration.NUM_SHARDS_CONFIG, 2);
        conf.set(MetricsConfiguration.METRICS_TABLE_CONFIG, "metricsTable");
        conf.set(MetricsConfiguration.ENABLED_LABELS_CONFIG, "table=shard,table=shardIndex,dataType=file,handler=" + DummyDataTypeHandler.class.getName());
        conf.set(MetricsConfiguration.FIELDS_CONFIG, "extension");
        conf.set(MetricsConfiguration.RECEIVERS_CONFIG, TestKeyValueCountMetricsReceiver.class.getName());
        
        context = new StandaloneTaskAttemptContext<>(conf, null);
        
        contextWriter = new TestContextWriter<>();
        MetricsService<BulkIngestKey,Value> metricsService = new MetricsService<>(contextWriter, context);
        kvContextWriter = new KeyValueCountingContextWriter<>(contextWriter, metricsService);
    }
    
    @Test
    public void shouldCreateKeyValueCountMetrics() throws Exception {
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), new Key("20160101_0", "fileIngest\0fileid1", ""));
        
        kvContextWriter.write(bik, new Value(), null);
        kvContextWriter.writeMetrics(fileEvent, fileFields, handler);
        kvContextWriter.cleanup(context);
        
        Multimap<BulkIngestKey,Value> written = contextWriter.getWritten();
        
        boolean foundGzMetric = false;
        boolean foundTarMetric = false;
        
        String expectedFamily = Metric.KV_PER_TABLE.toString();
        
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            String family = entry.getKey().getKey().getColumnFamily().toString();
            String qualifier = entry.getKey().getKey().getColumnQualifier().toString();
            
            System.out.println(entry);
            
            foundGzMetric = foundGzMetric || (family.equals(expectedFamily) && qualifier.equals("extension\u0000gz\u0000shard"));
            foundTarMetric = foundTarMetric || (family.equals(expectedFamily) && qualifier.equals("extension\u0000tar\u0000shard"));
        }
        
        assertTrue("Did not find .gz KeyValueCount mutation", foundGzMetric);
        assertTrue("Did not find .tar KeyValueCount mutation", foundTarMetric);
    }
    
    @Test
    public void shouldDropUnconfiguredTableMutations() throws Exception {
        // this should be recorded
        BulkIngestKey inserted = new BulkIngestKey(new Text("shard"), new Key("20160101_0", "fileIngest\0fileid1", ""));
        Multimap<String,NormalizedContentInterface> insertedFields = createFields("extension", "txt");
        kvContextWriter.write(inserted, new Value(), null);
        kvContextWriter.writeMetrics(fileEvent, insertedFields, handler);
        
        // this should not
        BulkIngestKey ignored = new BulkIngestKey(new Text("edges"), new Key("20160101_1", "fileIngest\0fileid2", ""));
        Multimap<String,NormalizedContentInterface> ignoredFields = createFields("extension", "gz");
        kvContextWriter.write(ignored, new Value(), null);
        kvContextWriter.writeMetrics(fileEvent, ignoredFields, handler);
        kvContextWriter.cleanup(context);
        
        Multimap<BulkIngestKey,Value> written = contextWriter.getWritten();
        
        // two original mutations plus one metric
        assertEquals(3, written.size());
        
        Map.Entry<BulkIngestKey,Value> entry = findByFamily(written, Metric.KV_PER_TABLE.toString());
        assertEquals("extension\u0000txt\u0000shard", qualifier(entry));
    }
    
    private Map.Entry<BulkIngestKey,Value> findByFamily(Multimap<BulkIngestKey,Value> pairs, String exptectedFamily) {
        for (Map.Entry<BulkIngestKey,Value> entry : pairs.entries()) {
            if (exptectedFamily.equals(family(entry))) {
                return entry;
            }
        }
        return null;
    }
}
