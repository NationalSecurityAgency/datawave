package datawave.ingest.mapreduce;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.hash.UIDConstants;
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
        record.setTimestamp(eventTime);
        record.setRawFileName("/some/filename");
        record.setRawData("some data".getBytes());
        record.generateId(null);

        errorRecord = new SimpleRawRecord();
        errorRecord.setRawFileTimestamp(0);
        errorRecord.setDataType(type);
        errorRecord.setTimestamp(eventTime);
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

    public abstract static class TestPredicate implements RawRecordPredicate {
        public static ThreadLocal<Set<String>> seenTypes = ThreadLocal.withInitial(() -> new HashSet<>());
        public static ThreadLocal<Set<RawRecordContainer>> allowed = ThreadLocal.withInitial(() -> new HashSet<>());
        public static ThreadLocal<Set<RawRecordContainer>> denied = ThreadLocal.withInitial(() -> new HashSet<>());

        public static void reset() {
            seenTypes.get().clear();
            allowed.get().clear();
            denied.get().clear();
        }

        @Override
        public void setConfiguration(String type, Configuration conf) {
            seenTypes.get().add(type);
        }

        @Override
        public boolean test(RawRecordContainer record) {
            boolean value = RawRecordPredicate.super.test(record);
            if (value) {
                allowed.get().add(record);
            } else {
                denied.get().add(record);
            }
            return value;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }
    }

    public static class AlwaysProcessPredicate extends TestPredicate {
        @Override
        public boolean shouldProcess(RawRecordContainer record) {
            return true;
        }
    }

    public static class DroppingAllPredicate extends TestPredicate {
        @Override
        public boolean shouldProcess(RawRecordContainer record) {
            return false;
        }
    }

    public static class CantConfigureEventPredicate extends AlwaysProcessPredicate implements RawRecordPredicate {
        @Override
        public void setConfiguration(String type, Configuration conf) {
            throw new RuntimeException();
        }
    }

    @Test
    public void testBaseEventPredicates() throws IOException, InterruptedException {
        try {
            conf.set(EventMapper.RECORD_PREDICATES, AlwaysProcessPredicate.class.getName());
            eventMapper.setup(mapContext);
            eventMapper.map(new LongWritable(1), record, mapContext);
            eventMapper.cleanup(mapContext);
            Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
            assertEquals(5, written.size());
            assertEquals(Sets.newHashSet("all", "file"), TestPredicate.seenTypes.get());
            assertEquals(0, TestPredicate.denied.get().size());
            assertEquals(1, TestPredicate.allowed.get().size());
        } finally {
            TestPredicate.reset();
        }
    }

    @Test
    public void testTypePredicates() throws IOException, InterruptedException {
        try {
            conf.set("file." + EventMapper.RECORD_PREDICATES, AlwaysProcessPredicate.class.getName());
            eventMapper.setup(mapContext);
            eventMapper.map(new LongWritable(1), record, mapContext);
            eventMapper.cleanup(mapContext);
            Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
            assertEquals(5, written.size());
            assertEquals(Sets.newHashSet("file"), TestPredicate.seenTypes.get());
            assertEquals(0, TestPredicate.denied.get().size());
            assertEquals(1, TestPredicate.allowed.get().size());
        } finally {
            TestPredicate.reset();
        }
    }

    @Test
    public void testMultTypePredicates() throws IOException, InterruptedException {
        try {
            conf.set("file." + EventMapper.RECORD_PREDICATES, AlwaysProcessPredicate.class.getName() + "," + DroppingAllPredicate.class.getName());
            eventMapper.setup(mapContext);
            eventMapper.map(new LongWritable(1), record, mapContext);
            eventMapper.cleanup(mapContext);
            Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();
            assertEquals(0, written.size());
            assertEquals(Sets.newHashSet("file"), TestPredicate.seenTypes.get());
            assertEquals(1, TestPredicate.denied.get().size());
            // allowed size may be 1 or 0, depending on the order of the predicates in the set. No need to test allowed size.
        } finally {
            TestPredicate.reset();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailedPredicates() throws IOException, InterruptedException {
        try {
            conf.set("file." + EventMapper.RECORD_PREDICATES, CantConfigureEventPredicate.class.getName());
            eventMapper.setup(mapContext);
            eventMapper.map(new LongWritable(1), record, mapContext);
        } finally {
            TestPredicate.reset();
        }
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

    @Test
    public void testHitDiscardInterval() throws IOException, InterruptedException {
        // event date < now minus discard interval
        long yesterday = -1L * UIDConstants.MILLISECONDS_PER_DAY;
        conf.setLong(record.getDataType().typeName() + "." + DataTypeDiscardIntervalPredicate.DISCARD_INTERVAL, yesterday);

        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);

        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();

        // discard interval lower bound is yesterday. data from today should be dropped
        assertEquals(0, written.size());
    }

    @Test
    public void testMissDiscardInterval() throws IOException, InterruptedException {
        // event date < now minus discard interval
        long tomorrow = 1L * UIDConstants.MILLISECONDS_PER_DAY;
        conf.setLong(record.getDataType().typeName() + "." + DataTypeDiscardIntervalPredicate.DISCARD_INTERVAL, tomorrow);

        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);

        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();

        // discard interval lower bound is tomorrow. data from today should be ok
        assertEquals(5, written.size());
    }

    @Test
    public void testHitFutureDiscardInterval() throws IOException, InterruptedException {
        // event date > now plus future discard interval
        long yesterday = -1L * UIDConstants.MILLISECONDS_PER_DAY;
        conf.setLong(record.getDataType().typeName() + "." + DataTypeDiscardFutureIntervalPredicate.DISCARD_FUTURE_INTERVAL, yesterday);

        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);

        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();

        // future discard is yesterday. data from today should be dropped
        assertEquals(0, written.size());

    }

    @Test
    public void testMissFutureDiscardInterval() throws IOException, InterruptedException {
        // event date > now plus future discard interval
        long tomorrow = 1L * UIDConstants.MILLISECONDS_PER_DAY;
        conf.setLong(record.getDataType().typeName() + "." + DataTypeDiscardFutureIntervalPredicate.DISCARD_FUTURE_INTERVAL, tomorrow);

        eventMapper.setup(mapContext);
        eventMapper.map(new LongWritable(1), record, mapContext);
        eventMapper.cleanup(mapContext);

        Multimap<BulkIngestKey,Value> written = TestContextWriter.getWritten();

        // future discard is tomorrow. data from today should be ok
        assertEquals(5, written.size());

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
