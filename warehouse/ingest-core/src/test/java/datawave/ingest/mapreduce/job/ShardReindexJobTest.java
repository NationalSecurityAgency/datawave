package datawave.ingest.mapreduce.job;

import static datawave.ingest.data.config.CSVHelper.DATA_HEADER;
import static datawave.ingest.data.config.CSVHelper.DATA_SEP;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_FIELDS;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.REVERSE_INDEX_FIELDS;
import static datawave.ingest.mapreduce.job.ShardReindexJob.FI_END;
import static datawave.ingest.mapreduce.job.ShardReindexJob.FI_START;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.protobuf.TermWeight;

public class ShardReindexJobTest extends EasyMockSupport {
    private ShardReindexJob job;
    private Configuration conf;
    private Mapper.Context context;
    private RecordWriter mockWriter;
    private StatusReporter statusReporter;

    @Before
    public void setup() {

        job = new ShardReindexJob();

        // clear and reset the type registry
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));

        // base data type config
        conf.set("samplecsv" + DATA_HEADER, "a,b,c,d,e");
        conf.set("samplecsv" + DATA_SEP, ",");
        conf.set("samplecsv" + INDEX_FIELDS, "FIELDA,FIELDB,FIELDC");
        conf.set("samplecsv" + REVERSE_INDEX_FIELDS, "FIELDB,FIELDD");
        Type t = new Type("samplecsv", CSVIngestHelper.class, null, null, 0, null);

        // add the type to the registry for use
        TypeRegistry.reset();
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(t.typeName(), t);

        // always prevent timestamp adjustments
        conf.setBoolean("floorTimestamps", false);
        conf.set("", "datawave.mr.bulk.MultiRfileInputFormat");
    }

    private void createContext() {
        mockWriter = createMock(RecordWriter.class);
        statusReporter = new StandaloneStatusReporter();
        WrappedMapper mapper = new WrappedMapper<>();
        context = mapper.getMapContext(new MapContextImpl(conf, new TaskAttemptID(), null, mockWriter, null, statusReporter, null));
    }

    private Collection<Range> buildRanges(String row, int shards) {
        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < shards; i++) {
            Text shardRow = new Text(row + "_" + i);
            ranges.add(new Range(new Key(shardRow, FI_START), true, new Key(shardRow, FI_END), true));
        }

        return ranges;
    }

    private void verifyRanges(Collection<Range> ranges, Collection<Range> expected) {
        Iterator<Range> rangeIterator = ranges.iterator();
        for (Range expectedRange : expected) {
            Assert.assertTrue(rangeIterator.hasNext());
            Assert.assertEquals(expectedRange, rangeIterator.next());
        }

        Assert.assertFalse(rangeIterator.hasNext());
    }

    @Test
    public void oneDayRange_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildRanges("20230925", "20230925", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230925", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void twoDayRange_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildRanges("20230925", "20230926", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230925", 5));
        expected.addAll(buildRanges("20230926", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void oneWeekRange_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildRanges("20230901", "20230907", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230901", 5));
        expected.addAll(buildRanges("20230902", 5));
        expected.addAll(buildRanges("20230903", 5));
        expected.addAll(buildRanges("20230904", 5));
        expected.addAll(buildRanges("20230905", 5));
        expected.addAll(buildRanges("20230906", 5));
        expected.addAll(buildRanges("20230907", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void monthRollover_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildRanges("20230831", "20230901", 5);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230831", 5));
        expected.addAll(buildRanges("20230901", 5));

        verifyRanges(ranges, expected);
    }

    @Test
    public void singleSplit_test() throws ParseException {
        replayAll();

        Collection<Range> ranges = ShardReindexJob.buildRanges("20230831", "20230831", 1);

        verifyAll();

        List<Range> expected = new ArrayList<>();
        expected.addAll(buildRanges("20230831", 1));

        verifyRanges(ranges, expected);
    }

    @Test
    public void mapperUnindexed_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        createContext();

        replayAll();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperUnindexedCleanup_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("job.cleanupShard", true);
        createContext();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        Key deletedkey = new Key(fiKey);
        deletedkey.setDeleted(true);
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), deletedkey);

        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik)), EasyMock.and(EasyMock.isA(Value.class), EasyMock.eq(new Value())));

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperForwardIndex_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("job.cleanupShard", true);
        createContext();

        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "samplecsv", 1000l);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);

        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik)), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperForwardAndReverseIndex_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("job.cleanupShard", true);
        createContext();

        Key fiKey = new Key("row", FI_START + "FIELDB", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        Key indexKey = new Key("ABC", "FIELDB", "row" + '\u0000' + "samplecsv", 1000l);
        Key revKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv", 1000l);

        BulkIngestKey bik1 = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik1)), EasyMock.isA(Value.class));
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik2)), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperReverseIndex_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("job.cleanupShard", true);
        createContext();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv", 1000l);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik2)), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperDeleteKey_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("propagateDeletes", true);
        conf.setBoolean("job.cleanupShard", true);
        createContext();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        fiKey.setDeleted(true);
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv", 1000l);
        revKey.setDeleted(true);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik2)), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperDeleteKeyNoPropagate_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("propagateDeletes", false);
        conf.setBoolean("job.cleanupShard", true);
        createContext();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", 1000l);
        fiKey.setDeleted(true);
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperFloorTimestamps_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("floorTimestamps", true);
        createContext();

        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);
        long timestamp = calendar.getTimeInMillis();
        calendar.set(year, month, day, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long floorTime = calendar.getTimeInMillis();

        // create the key with no timestamp
        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", timestamp);
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv", floorTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik)), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperEventNotProcessingEvent_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        createContext();

        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", 1000l);

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapperEventProcessingEventNoDefaultDataType_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("reprocessEvents", true);
        createContext();

        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", 1000l);

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapperEventProcessingEventNoDataTypeHandler_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("reprocessEvents", true);
        conf.set("defaultDataType", "samplecsv");
        createContext();

        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", 1000l);

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapperEventProcessingEventNoNumShards_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("reprocessEvents", true);
        conf.set("defaultDataType", "samplecsv");
        conf.set("dataTypeHandler", "datawave.ingest.mapreduce.job.ShardReindexJobTest$TestDataTypeHandler");
        createContext();

        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", 1000l);

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test(expected = IllegalStateException.class)
    public void mapperEventProcessingEventNoEventClass_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("reprocessEvents", true);
        conf.set("defaultDataType", "samplecsv");
        conf.set("dataTypeHandler", "datawave.ingest.mapreduce.job.ShardReindexJobTest$TestDataTypeHandler");
        conf.setInt("num.shards", 3);
        createContext();

        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", 1000l);

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperEventProcessingEvent_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("reprocessEvents", true);
        conf.set("defaultDataType", "samplecsv");
        conf.set("dataTypeHandler", "datawave.ingest.mapreduce.job.ShardReindexJobTest$TestDataTypeHandler");
        conf.setInt("num.shards", 3);
        conf.set("eventClass", RawRecordContainerImpl.class.getCanonicalName());

        // for process bulk
        conf.set("shard.table.name", "shard");
        conf.set("shard.global.index.table.name", "shardIndex");
        conf.set("shard.global.rindex.table.name", "shardReverseIndex");

        createContext();

        // create a timestamp for today
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);
        long timestamp = calendar.getTimeInMillis();
        calendar.set(year, month, day, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long floorTime = calendar.getTimeInMillis();

        Key event = new Key("20231229_2", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", timestamp);

        Key fiKey = new Key("20231229_2", "fi" + '\u0000' + "FIELDB", "my field value" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", timestamp);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(fiBik)), EasyMock.isA(Value.class));

        Key indexKey = new Key("my field value", "FIELDB", "20231229_2" + '\u0000' + "samplecsv", floorTime);
        BulkIngestKey giBik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(giBik)), EasyMock.isA(Value.class));

        Key revKey = new Key("eulav dleif ym", "FIELDB", "20231229_2" + '\u0000' + "samplecsv", floorTime);
        BulkIngestKey griBik = new BulkIngestKey(new Text("shardReverseIndex"), revKey);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(griBik)), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperEventProcessingEventWithTokens_test() throws IOException, InterruptedException {
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();
        conf.setBoolean("reprocessEvents", true);
        conf.set("defaultDataType", "samplecsv");
        conf.set("dataTypeHandler", "datawave.ingest.mapreduce.job.ShardReindexJobTest$TestDataTypeHandler");
        conf.setInt("num.shards", 3);
        conf.set("eventClass", RawRecordContainerImpl.class.getCanonicalName());

        // for process bulk
        conf.set("shard.table.name", "shard");
        conf.set("shard.global.index.table.name", "shardIndex");
        conf.set("shard.global.rindex.table.name", "shardReverseIndex");

        // add tokenization to FIELDB
        conf.set("samplecsv.data.category.index.tokenize.allowlist", "FIELDB");

        createContext();

        // create a fixed timestamp based off now
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, 11, 29);
        long timestamp = calendar.getTimeInMillis();
        calendar.set(2023, 11, 29, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long floorTime = calendar.getTimeInMillis();

        Key event = new Key("20231229_2", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field value", timestamp);

        Key fiKey = new Key("20231229_2", "fi" + '\u0000' + "FIELDB", "my field value" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3", timestamp);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(fiBik)), EasyMock.isA(Value.class));

        Key indexKey = new Key("my field value", "FIELDB", "20231229_2" + '\u0000' + "samplecsv", floorTime);
        BulkIngestKey giBik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(giBik)), EasyMock.isA(Value.class));

        Key revKey = new Key("eulav dleif ym", "FIELDB", "20231229_2" + '\u0000' + "samplecsv", floorTime);
        BulkIngestKey griBik = new BulkIngestKey(new Text("shardReverseIndex"), revKey);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(griBik)), EasyMock.isA(Value.class));

        // tokens
        Capture<Value> myTokenInfo = expectTokens("20231229_2", "samplecsv", "1.2.3", timestamp, floorTime, "FIELDB", "my");
        Capture<Value> fieldTokenInfo = expectTokens("20231229_2", "samplecsv", "1.2.3", timestamp, floorTime, "FIELDB", "field");
        Capture<Value> valueTokenInfo = expectTokens("20231229_2", "samplecsv", "1.2.3", timestamp, floorTime, "FIELDB", "value");

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        TermWeight.Info info = TermWeight.Info.parseFrom(myTokenInfo.getValue().get());
        Assert.assertEquals(1, info.getTermOffsetCount());
        Assert.assertEquals(1, info.getTermOffset(0));

        info = TermWeight.Info.parseFrom(fieldTokenInfo.getValue().get());
        Assert.assertEquals(1, info.getTermOffsetCount());
        Assert.assertEquals(2, info.getTermOffset(0));

        info = TermWeight.Info.parseFrom(valueTokenInfo.getValue().get());
        Assert.assertEquals(1, info.getTermOffsetCount());
        Assert.assertEquals(3, info.getTermOffset(0));

        verifyAll();
    }

    private Capture<Value> expectTokens(String shard, String dataType, String uid, long origTime, long floorTime, String field, String value)
                    throws IOException, InterruptedException {
        Key fiToken1 = new Key(shard, "fi" + '\u0000' + field + "_TOKEN", value + '\u0000' + dataType + '\u0000' + uid, origTime);
        BulkIngestKey fiToken1Bik = new BulkIngestKey(new Text("shard"), fiToken1);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(fiToken1Bik)), EasyMock.isA(Value.class));

        Key indexToken1 = new Key(value, field + "_TOKEN", shard + '\u0000' + dataType, floorTime);
        BulkIngestKey indexToken1Bik = new BulkIngestKey(new Text("shardIndex"), indexToken1);
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(indexToken1Bik)), EasyMock.isA(Value.class));

        Key tfToken1 = new Key(shard, "tf", dataType + '\u0000' + uid + '\u0000' + value + '\u0000' + field + "_TOKEN", origTime);
        BulkIngestKey tfToken1Bik = new BulkIngestKey(new Text("shard"), tfToken1);
        Capture<Value> offsetValue = EasyMock.newCapture();
        mockWriter.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(tfToken1Bik)),
                        EasyMock.and(EasyMock.isA(Value.class), EasyMock.capture(offsetValue)));
        return offsetValue;
    }

    public static class TestDataTypeHandler extends ContentIndexingColumnBasedHandler {

        @Override
        public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
            return new CSVIngestHelper();
        }
    }
}
