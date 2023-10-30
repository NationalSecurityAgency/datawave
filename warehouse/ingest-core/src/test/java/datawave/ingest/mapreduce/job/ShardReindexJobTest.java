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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.data.RawRecordContainerImplTest;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.ingest.CSVIngestHelper;

public class ShardReindexJobTest extends EasyMockSupport {
    private ShardReindexJob job;
    private Configuration conf;

    @Before
    public void setup() {

        job = new ShardReindexJob();

        // clear and reset the type registry
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));

        conf.set("samplecsv" + DATA_HEADER, "a,b,c,d,e");
        conf.set("samplecsv" + DATA_SEP, ",");
        conf.set("samplecsv" + INDEX_FIELDS, "FIELDA,FIELDB,FIELDC");
        conf.set("samplecsv" + REVERSE_INDEX_FIELDS, "FIELDB,FIELDD");

        Type t = new Type("samplecsv", CSVIngestHelper.class, null, null, 0, null);

        TypeRegistry.reset();
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(t.typeName(), t);
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
    public void mapperUnindexed_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();

        EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();

        replayAll();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperUnindexedCleanup_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        Counter mockCounter = createMock(Counter.class);
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key deletedkey = new Key(fiKey);
        deletedkey.setDeleted(true);
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), deletedkey);

        EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();
        context.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik)), EasyMock.and(EasyMock.isA(Value.class), EasyMock.eq(new Value())));
        EasyMock.expect(context.getCounter("cleanup", "fi")).andReturn(mockCounter);
        mockCounter.increment(1l);

        replayAll();

        // enable cleanup keys
        conf.setBoolean("job.cleanupShard", true);

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperForwardIndex_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();

        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik)), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean("job.cleanupShard", true);
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperForwardAndReverseIndex_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();

        Key fiKey = new Key("row", FI_START + "FIELDB", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDB", "row" + '\u0000' + "samplecsv");
        Key revKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");

        BulkIngestKey bik1 = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik1)), EasyMock.isA(Value.class));
        context.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik2)), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean("job.cleanupShard", true);
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void mapperReverseIndex_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexJob.FiToGiMapper mapper = new ShardReindexJob.FiToGiMapper();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik2)), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean("job.cleanupShard", true);
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }
}
