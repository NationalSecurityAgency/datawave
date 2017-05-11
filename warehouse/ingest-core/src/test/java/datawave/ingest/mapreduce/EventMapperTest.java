package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.metrics.*;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

public class EventMapperTest {
    
    private Configuration conf;
    private MapDriver<LongWritable,RawRecordContainer,BulkIngestKey,Value> driver;
    private SimpleRawRecord record;
    
    @Before
    public void setUp() {
        long eventTime = System.currentTimeMillis();
        
        EventMapper<LongWritable,RawRecordContainer,BulkIngestKey,Value> mapper = new EventMapper<>();
        driver = new MapDriver<>(mapper);
        
        conf = driver.getConfiguration();
        conf.setClass(EventMapper.CONTEXT_WRITER_CLASS, TestContextWriter.class, ContextWriter.class);
        
        Type type = new Type("file", null, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(type.typeName(), type);
        
        Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
        fields.put("fileExtension", new BaseNormalizedContent("fileExtension", "gz"));
        fields.put("lastModified", new BaseNormalizedContent("lastModified", "2016-01-01"));
        
        SimpleDataTypeHelper.registerFields(fields);
        
        record = new SimpleRawRecord();
        record.setRawFileTimestamp(eventTime);
        record.setDataType(type);
        record.setDate(eventTime);
        record.setRawDataAndGenerateId("some data".getBytes());
    }
    
    @Test
    public void shouldNotWriteMetricsByDefault() throws IOException {
        driver.setInput(new LongWritable(1), record);
        driver.run();
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE
        assertEquals(4, written.size());
        
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            assertEquals(SimpleDataTypeHandler.TABLE, entry.getKey().getTableName());
        }
    }
    
    @Test
    public void shouldWriteMetricsWhenConfigured() throws IOException {
        String metricsTable = "ingestMetrics";
        
        // configure metrics
        conf.setBoolean(MetricsConfiguration.METRICS_ENABLED_CONFIG, true);
        conf.setInt(MetricsConfiguration.NUM_SHARDS_CONFIG, 1);
        conf.set(MetricsConfiguration.METRICS_TABLE_CONFIG, metricsTable);
        conf.set(MetricsConfiguration.ENABLED_LABELS_CONFIG,
                        "table=" + SimpleDataTypeHandler.TABLE + ",dataType=file,handler=" + SimpleDataTypeHandler.class.getName());
        conf.set(MetricsConfiguration.FIELDS_CONFIG, "fileExtension");
        conf.set(MetricsConfiguration.RECEIVERS_CONFIG, TestEventCountMetricsReceiver.class.getName());
        
        driver.setInput(new LongWritable(1), record);
        driver.run();
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE + one metric
        assertEquals(5, written.size());
        
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            System.out.println(entry);
        }
        
        Map.Entry<BulkIngestKey,Value> entry = getMetric(written);
        
        assertNotNull(entry);
        assertEquals(metricsTable, entry.getKey().getTableName().toString());
        assertEquals("fileExtension\u0000gz", entry.getKey().getKey().getColumnQualifier().toString());
    }
    
    private Map.Entry<BulkIngestKey,Value> getMetric(Multimap<BulkIngestKey,Value> written) {
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            String fam = entry.getKey().getKey().getColumnFamily().toString();
            if (fam.equals(Metric.EVENT_COUNT.toString())) {
                return entry;
            }
        }
        return null;
    }
}
