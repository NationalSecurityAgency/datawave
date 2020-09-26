package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.metrics.Metric;
import datawave.ingest.mapreduce.job.metrics.MetricsConfiguration;
import datawave.ingest.mapreduce.job.metrics.TestEventCountMetricsReceiver;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

public class EventMapperTest {
    
    @Rule
    public EasyMockRule easyMockRule = new EasyMockRule(this);
    
    @Mock
    private Mapper.Context mapContext;
    
    private Configuration conf;
    private SimpleRawRecord record;
    private SimpleRawRecord errorRecord;
    private EventMapper<LongWritable,RawRecordContainer,BulkIngestKey,Value> eventMapper;
    
    @Before
    public void setUp() throws Exception {
        long eventTime = System.currentTimeMillis();
        
        eventMapper = new EventMapper<>();
        conf = new Configuration();
        conf.setClass(EventMapper.CONTEXT_WRITER_CLASS, TestContextWriter.class, ContextWriter.class);
        
        Type type = new Type("file", null, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
        Type errorType = new Type(TypeRegistry.ERROR_PREFIX, null, null, new String[] {SimpleDataTypeHandler.class.getName()}, 20, null);
        
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(type.typeName(), type);
        registry.put(errorType.typeName(), errorType);
        
        Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
        fields.put("fileExtension", new BaseNormalizedContent("fileExtension", "gz"));
        fields.put("lastModified", new BaseNormalizedContent("lastModified", "2016-01-01"));
        
        SimpleDataTypeHelper.registerFields(fields);
        
        record = new SimpleRawRecord();
        record.setRawFileTimestamp(eventTime);
        record.setDataType(type);
        record.setDate(eventTime);
        record.setRawFileName("/some/filename");
        record.setRawData("some data".getBytes());
        record.generateId(null);
        
        errorRecord = new SimpleRawRecord();
        errorRecord.setRawFileTimestamp(0);
        errorRecord.setDataType(type);
        errorRecord.setDate(eventTime);
        errorRecord.setRawFileName("/some/filename");
        errorRecord.setRawData("some data".getBytes());
        errorRecord.generateId(null);
        errorRecord.setRawFileName("");
        errorRecord.addError("EVENT_DATE_MISSING");
        errorRecord.setFatalError(true);
        
        expect(mapContext.getConfiguration()).andReturn(conf).anyTimes();
        
        mapContext.progress();
        expectLastCall().anyTimes();
        
        TestContextWriter<BulkIngestKey,Value> testContextWriter = new TestContextWriter<>();
        mapContext.write(anyObject(BulkIngestKey.class), anyObject(Value.class));
        expectLastCall().andDelegateTo(testContextWriter).anyTimes();
        
        expect(mapContext.getInputSplit()).andReturn(null);
        expect(mapContext.getMapOutputValueClass()).andReturn(null);
        
        TaskAttemptID id = new TaskAttemptID();
        expect(mapContext.getTaskAttemptID()).andReturn(id).anyTimes();
        
        StandaloneTaskAttemptContext standaloneContext = new StandaloneTaskAttemptContext(conf, new StandaloneStatusReporter());
        expect(mapContext.getCounter(anyObject())).andDelegateTo(standaloneContext).anyTimes();
        expect(mapContext.getCounter(anyString(), anyString())).andDelegateTo(standaloneContext).anyTimes();
        
        replay(mapContext);
    }
    
    @After
    public void checkMock() {
        verify(mapContext);
    }
    
    @Test
    public void shouldHandleNullRawData() throws IOException, InterruptedException {
        // some RecordReaders may null out raw data entirely because they pass data to their
        // handlers in other ways. Verify that the EventMapper can handle this case.
        record.setRawData(null);
        
        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE + RAW_FILE
        assertEquals(5, written.size());
    }
    
    @Test
    public void shouldNotWriteMetricsByDefault() throws IOException, InterruptedException {
        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE + RAW_FILE
        assertEquals(5, written.size());
        
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            assertEquals(SimpleDataTypeHandler.TABLE, entry.getKey().getTableName());
        }
    }
    
    @Test
    public void shouldWriteMetricsWhenConfigured() throws IOException, InterruptedException {
        String metricsTable = "ingestMetrics";
        
        // configure metrics
        conf.setBoolean(MetricsConfiguration.METRICS_ENABLED_CONFIG, true);
        conf.setInt(MetricsConfiguration.NUM_SHARDS_CONFIG, 1);
        conf.set(MetricsConfiguration.METRICS_TABLE_CONFIG, metricsTable);
        conf.set(MetricsConfiguration.ENABLED_LABELS_CONFIG,
                        "table=" + SimpleDataTypeHandler.TABLE + ",dataType=file,handler=" + SimpleDataTypeHandler.class.getName());
        conf.set(MetricsConfiguration.FIELDS_CONFIG, "fileExtension");
        conf.set(MetricsConfiguration.RECEIVERS_CONFIG, TestEventCountMetricsReceiver.class.getName());
        
        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE + RAW_FILE + one metric
        assertEquals(6, written.size());
        
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            System.out.println(entry);
        }
        
        Map.Entry<BulkIngestKey,Value> entry = getMetric(written);
        
        assertNotNull(entry);
        assertEquals(metricsTable, entry.getKey().getTableName().toString());
        assertEquals("fileExtension\u0000gz", entry.getKey().getKey().getColumnQualifier().toString());
        
        entry = getRawFileName(written);
        assertEquals("/some/filename", entry.getKey().getKey().getColumnQualifier().toString());
    }
    
    @Test
    public void shouldNotWriteRawFile() throws IOException, InterruptedException {
        record.setRawFileName("");
        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE
        assertEquals(4, written.size());
    }
    
    @Test
    public void errorEventWithZeroTimestampNotDropped() throws IOException, InterruptedException {
        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), errorRecord, mapContext);
        eventMapper.cleanup(mapContext);
        
        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
        
        // two fields mutations + LOAD_DATE + ORIG_FILE
        // previously this would have been zero
        assertEquals(4, written.size());
    }
    
    private Map.Entry<BulkIngestKey,Value> getMetric(Multimap<BulkIngestKey,Value> written) {
        return getFieldEntry(written, Metric.EVENT_COUNT.toString());
    }
    
    private Map.Entry<BulkIngestKey,Value> getRawFileName(Multimap<BulkIngestKey,Value> written) {
        return getFieldEntry(written, EventMapper.RAW_FILE_FIELDNAME.toString());
    }
    
    private Map.Entry<BulkIngestKey,Value> getFieldEntry(Multimap<BulkIngestKey,Value> written, String field) {
        for (Map.Entry<BulkIngestKey,Value> entry : written.entries()) {
            String fam = entry.getKey().getKey().getColumnFamily().toString();
            if (fam.equals(field)) {
                return entry;
            }
        }
        return null;
    }
    
}
