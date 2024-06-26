package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.data.config.CSVHelper.DATA_HEADER;
import static datawave.ingest.data.config.CSVHelper.DATA_SEP;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_FIELDS;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_ONLY_FIELDS;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.REVERSE_INDEX_FIELDS;
import static datawave.ingest.data.config.ingest.ContentBaseIngestHelper.TOKEN_INDEX_ALLOWLIST;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.METADATA_TABLE_NAME;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.SHARD_GIDX_TNAME;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.SHARD_GRIDX_TNAME;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.SHARD_TNAME;
import static datawave.ingest.mapreduce.job.reindex.ShardReindexJob.FI_START;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.isA;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.shaded.com.google.common.io.Files;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.data.hash.HashUID;
import datawave.data.type.NoOpType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.util.RFileUtil;
import datawave.ingest.protobuf.TermWeight;

public class ShardReindexMapperTest extends EasyMockSupport {
    private Configuration conf;
    private ShardIdFactory shardIdFactory;

    @Before
    public void setup() {
        // TODO shift this to ShardedDataGenerator
        // clear and reset the type registry
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/error-ingest-config.xml"));

        // ShardedDataTypeHandler config
        conf.setInt("num.shards", 8);
        conf.set(SHARD_TNAME, "shard");
        conf.set(SHARD_GIDX_TNAME, "shardIndex");
        conf.set(SHARD_GRIDX_TNAME, "shardReverseIndex");
        conf.set(METADATA_TABLE_NAME, "DatawaveMetadata");

        // simple required config for a type with some indexed fields
        conf.set("samplecsv" + DATA_HEADER, "a,b,c,d,e");
        conf.set("samplecsv" + DATA_SEP, ",");
        conf.set("samplecsv" + INDEX_FIELDS, "FIELDA,FIELDB,FIELDC,FIELDE,FIELDE_TOKEN,FIELDF,FIELDF_TOKEN,FIELDG,FIELDG_TOKEN");
        conf.set("samplecsv" + REVERSE_INDEX_FIELDS, "FIELDB,FIELDD");
        conf.set("samplecsv" + INDEX_ONLY_FIELDS, "FIELDE,FIELDE_TOKEN");
        conf.set("samplecsv" + TOKEN_INDEX_ALLOWLIST, "FIELDE,FIELDF,FIELDG");

        Type t = new Type("samplecsv", CSVIngestHelper.class, null, null, 0, null);
        // this needs to be called each test to clear any static config that may be cached
        t.clearIngestHelper();

        TypeRegistry.reset();
        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(t.typeName(), t);

        shardIdFactory = new ShardIdFactory(conf);

        // disable timestamp
        conf.setBoolean(ShardReindexMapper.FLOOR_TIMESTAMPS, false);
    }

    @Test
    public void FI_unindexed_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();

        replayAll();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_unindexed_export_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        conf.setBoolean(ShardReindexMapper.EXPORT_SHARD, true);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();

        replayAll();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_unindexedCleanup_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key deletedkey = new Key(fiKey);
        deletedkey.setDeleted(true);
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), deletedkey);
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_forwardIndex_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_forwardIndex_export_notIndexOnly_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_forwardIndex_export_indexOnly_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDE", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDE", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        // write both the shardIndex and the original fi
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.write(EasyMock.eq(fiBik), EasyMock.isA(Value.class));
        context.progress();

        conf.setBoolean(ShardReindexMapper.ENABLE_REINDEX_COUNTERS, true);
        conf.setBoolean(ShardReindexMapper.DUMP_COUNTERS, true);

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void FI_forwardAndReverseIndex_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDB", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDB", "row" + '\u0000' + "samplecsv");
        Key revKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");

        BulkIngestKey bik1 = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.eq(bik1), EasyMock.isA(Value.class));
        context.write(EasyMock.eq(bik2), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_reverseIndex_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.and(EasyMock.isA(BulkIngestKey.class), EasyMock.eq(bik2)), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_deletedKey_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        fiKey.setDeleted(true);
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv");
        revKey.setDeleted(true);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.eq(bik2), EasyMock.isA(Value.class));
        context.progress();

        conf.setBoolean(ShardReindexMapper.FLOOR_TIMESTAMPS, false);
        conf.setBoolean(ShardReindexMapper.PROPAGATE_DELETES, true);

        replayAll();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void FI_deletedKey_noPropagate_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        fiKey.setDeleted(true);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        conf.setBoolean(ShardReindexMapper.PROPAGATE_DELETES, false);
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        context.progress();

        replayAll();

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_notProcessing_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();
        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field b value", 1000l);

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_unindexed_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELD_UNINDEXED" + '\u0000' + "my field b value", eventTime);

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        // output the event only
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), event);
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_forwardIndex_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        Key event = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA", "ABC", true);

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_forwardIndex_groupingNotation_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        Key event = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA.123.234.345.456", "ABC", true);

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_forwardIndexTokenized_noTF_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        Key event = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "generate some tokens", new String[] {"generate", "some", "tokens"},
                        false);

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_forwardIndexTokenized_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);

        ShardReindexMapper mapper = new ShardReindexMapper();

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        Key event = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "generate some tokens", new String[] {"generate", "some", "tokens"},
                        true);

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    // all offsets are at 1 instead of being processed together
    @Test
    public void E_batchNone_multiValueToken_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);

        ShardReindexMapper mapper = new ShardReindexMapper();

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true);

        // ingest handler may call progress internally
        context.progress();
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);

        verifyAll();
    }

    // multivalued tokens are offset by being processed together but no cleanup called
    @Test
    public void E_batchField_multiValueToken_noCleanup_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        enableEventProcessing(true);
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event1 = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDF" + '\u0000' + "value1", eventTime);
        Key event2 = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDF" + '\u0000' + "value2", eventTime);

        // the original events should be all that is output
        BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event1);
        context.write(EasyMock.eq(eventBik), EasyMock.isA(Value.class));
        context.progress();
        eventBik = new BulkIngestKey(new Text("shard"), event2);
        context.write(EasyMock.eq(eventBik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_batchField_multiValueToken_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 11);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0);
        Key event3 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value3", new String[] {"value3"}, true, 22);

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.map(event3, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchField_multiValueToken_mixedVis_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 11, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");
        Key event3 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value3", new String[] {"value3"}, true, 0, "b");

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.map(event3, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchField_mixedEvent_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchField_tokenizedBatch_backToBack_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDG", "value1", new String[] {"value1"}, true);

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchEvent_tokenizedBatch_backToBack_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDG", "value1", new String[] {"value1"}, true);

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchEvent_mixedEvent_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchEvent_longMixedEvent_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3.4.6", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4.2.1", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchEvent_mixedEvents_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event1a = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value3", new String[] {"value3"}, true, 11, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");
        Key event2a = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value4", new String[] {"value4"}, true, 11, "a");

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event1a, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.map(event2a, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_batchField_tokenizedBatch_followingKey_test() throws ParseException, IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        ShardReindexMapper mapper = new ShardReindexMapper();

        // expect all keys for the tokenized field
        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA", "not tokenized", true);

        context.progress();
        // ingest handler may call progress internally
        EasyMock.expectLastCall().anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(EasyMock.isA(String.class), EasyMock.isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        replayAll();

        mapper.setup(context);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void E_reverseIndex_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDD" + '\u0000' + "ABC", eventTime);

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        // write a reverse global index key
        Key indexKey = new Key("CBA", "FIELDD", shard + '\u0000' + "samplecsv", eventTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardReverseIndex"), indexKey);
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));

        // DO NOT write an fi key (turns out this isn't expected)

        // write the event key
        BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event);
        context.write(EasyMock.eq(eventBik), EasyMock.isA(Value.class));

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_forwardAndReverseIndex_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDB" + '\u0000' + "this could be tokenized", eventTime);

        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();

        // write a global index key
        Key indexKey = new Key("this could be tokenized", "FIELDB", shard + '\u0000' + "samplecsv", eventTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));

        // write a reverse global index key
        Key rindexKey = new Key("dezinekot eb dluoc siht", "FIELDB", shard + '\u0000' + "samplecsv", eventTime);
        BulkIngestKey rbik = new BulkIngestKey(new Text("shardReverseIndex"), rindexKey);
        context.write(EasyMock.eq(rbik), EasyMock.isA(Value.class));

        // write an fi key
        Key fiKey = new Key(shard, FI_START + "FIELDB", "this could be tokenized" + '\u0000' + "samplecsv" + '\u0000' + uid, eventTime);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);
        context.write(EasyMock.eq(fiBik), EasyMock.isA(Value.class));

        // write the event key
        BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event);
        context.write(EasyMock.eq(eventBik), EasyMock.isA(Value.class));

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verifyAll();
    }

    @Test(expected = IllegalStateException.class)
    public void FI_unknownDataType_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();
        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "someUnknownDataType" + '\u0000' + "1.2.3");

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    // unlike unknownDataTypeFI_test() this uses the default data type to provide processing capability
    @Test
    public void FI_unknownDataType_wDefaultHelper_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        ShardReindexMapper mapper = new ShardReindexMapper();
        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "someUnknownDataType" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "someUnknownDataType");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);

        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void TF_noReprocess_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void TF_reprocess_notExported_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(false);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");

        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void TF_reprocess_indexOnlyNotGenerateTF_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), tfKey);

        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void TF_reprocess_indexOnlyGenerateTF_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), tfKey);

        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void TF_reprocess_notIndexOnlyNotGenerateTF_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDF");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), tfKey);

        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void TF_reprocess_notIndexOnlyGenerateTF_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDF");
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void D_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");

        Key dKey = new Key("row", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someViewName");
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(dKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void D_eventProcess_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(false);

        Key dKey = new Key("row", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someViewName");
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(dKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void D_eventProcessExport_test() throws IOException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        ShardReindexMapper mapper = new ShardReindexMapper();

        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(true);

        Key dKey = new Key("row", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someViewName");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), dKey);
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));
        context.progress();

        replayAll();

        mapper.setup(context);
        mapper.map(dKey, new Value(), context);

        verifyAll();
    }

    @Test
    public void E_forwardIndex_metadata_test() throws IOException, InterruptedException, ParseException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.GENERATE_METADATA, true);
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        ShardReindexMapper mapper = new ShardReindexMapper();

        Key event = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA", "ABC", true);
        context.progress();

        Key fKey = new Key("FIELDA", "f", "samplecsv" + '\u0000' + "20240216", event.getTimestamp());
        BulkIngestKey fBik = new BulkIngestKey(new Text("DatawaveMetadata"), fKey);
        context.write(EasyMock.eq(fBik), EasyMock.isA(Value.class));

        Key iKey = new Key("FIELDA", "i", "samplecsv" + '\u0000' + "20240216", event.getTimestamp());
        BulkIngestKey iBik = new BulkIngestKey(new Text("DatawaveMetadata"), iKey);
        context.write(EasyMock.eq(iBik), EasyMock.isA(Value.class));

        Key eKey = new Key("FIELDA", "e", "samplecsv", event.getTimestamp());
        BulkIngestKey eBik = new BulkIngestKey(new Text("DatawaveMetadata"), eKey);
        context.write(EasyMock.eq(eBik), EasyMock.isA(Value.class));

        Key tKey = new Key("FIELDA", "t", "samplecsv" + '\u0000' + NoOpType.class.getCanonicalName(), event.getTimestamp());
        BulkIngestKey tBik = new BulkIngestKey(new Text("DatawaveMetadata"), tKey);
        context.write(EasyMock.eq(tBik), EasyMock.isA(Value.class));

        replayAll();

        mapper.setup(context);
        mapper.map(event, new Value(), context);
        mapper.cleanup(context);

        verifyAll();
    }

    @Test
    public void createAndVerifyTest() throws IOException, ClassNotFoundException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);

        conf.setBoolean(ShardReindexMapper.ENABLE_REINDEX_COUNTERS, true);
        conf.setBoolean(ShardReindexMapper.DUMP_COUNTERS, true);
        conf.setBoolean(ShardReindexMapper.GENERATE_METADATA, true);
        enableEventProcessing(true);
        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        ShardReindexMapper mapper = new ShardReindexMapper();

        context.progress();
        expectLastCall().anyTimes();

        Multimap generated = TreeMultimap.create();

        context.write(isA(BulkIngestKey.class), isA(Value.class));
        expectLastCall().andAnswer(() -> {
            BulkIngestKey bik = (BulkIngestKey) getCurrentArguments()[0];
            Value value = (Value) getCurrentArguments()[1];
            generated.put(bik, new Value(value.get()));
            return null;
        }).anyTimes();

        // ingest handler may access counters, this is a catch all
        expect(context.getCounter(isA(String.class), isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        // other context for the verification mapper
        Mapper.Context verificationContext = createMock(Mapper.Context.class);
        expect(verificationContext.getCounter(isA(String.class), isA(String.class))).andReturn(new Counters.Counter()).anyTimes();

        verificationContext.progress();
        expectLastCall().anyTimes();

        replayAll();

        File inputFiles = null;
        try {
            List<String> dataOptions = new ArrayList<>();
            dataOptions.add("");
            dataOptions.add("red");
            dataOptions.add("green");
            dataOptions.add("yellow");
            dataOptions.add("blue");
            dataOptions.add("purple");
            dataOptions.add("all the colors of the rainbow");
            dataOptions.add("blues and reds");
            dataOptions.add("oranges and yellows");

            inputFiles = ShardedDataGenerator.createIngestFiles(dataOptions, 1);
            File shardFiles = new File(inputFiles, "shard/magic.rf");

            mapper.setup(context);

            RFile.Reader reader = RFileUtil.getRFileReader(conf, new Path(shardFiles.toString()));
            reader.seek(new Range(), Collections.emptySet(), false);
            System.out.println("processing mapper input");
            while (reader.hasTop()) {
                Key key = reader.getTopKey();
                System.out.println(key);
                Value value = reader.getTopValue();
                mapper.map(key, value, context);
                reader.next();
            }

            mapper.cleanup(context);

            System.out.println("processing diff");
            // write out the generated data
            File outDir = Files.createTempDir();
            try {
                ShardedDataGenerator.writeData(outDir.toString(), "reindexed.rf", generated);
                conf.set("source1", "FILE");
                conf.set("source1.files", shardFiles.toString());
                conf.set("source2", "FILE");
                conf.set("source2.files", outDir + "/shard/reindexed.rf");

                // read and compare the data
                ShardReindexVerificationMapper verificationMapper = new ShardReindexVerificationMapper();
                verificationMapper.setup(context);
                verificationMapper.map(new Range(), "", verificationContext);
            } finally {
                FileUtils.deleteDirectory(outDir);
            }
        } finally {
            if (inputFiles != null) {
                FileUtils.deleteDirectory(inputFiles);
            }
        }

        verifyAll();
    }

    private Key expectTokenized(Mapper.Context context, String date, String uid, String dataType, String field, String fullValue, String[] tokens,
                    boolean writeTF) throws ParseException, IOException, InterruptedException {
        return expectTokenized(context, date, uid, dataType, field, fullValue, tokens, writeTF, 0);
    }

    private Key expectTokenized(Mapper.Context context, String date, String uid, String dataType, String field, String fullValue, String[] tokens,
                    boolean writeTF, int tokenOffset) throws ParseException, IOException, InterruptedException {
        return expectTokenized(context, date, uid, dataType, field, fullValue, tokens, writeTF, tokenOffset, "");
    }

    private Key expectTokenized(Mapper.Context context, String date, String uid, String dataType, String field, String fullValue, String[] tokens,
                    boolean writeTF, int tokenOffset, String vis) throws ParseException, IOException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse(date);
        long eventTime = getTimestamp(d);
        String shard = getShard(d, uid);

        int offset = 1 + tokenOffset;

        Key event = expectIndexed(context, date, uid, dataType, field, fullValue, true, vis);

        for (String token : tokens) {
            String tokenField = field + "_TOKEN";
            expectIndexed(context, date, uid, dataType, tokenField, token, false, vis);
            // create the token
            if (writeTF) {
                Key tfKey = new Key(shard, "tf", dataType + '\u0000' + uid + '\u0000' + token + '\u0000' + tokenField, vis, eventTime);
                BulkIngestKey tfBik = new BulkIngestKey(new Text("shard"), tfKey);
                TermWeight.Info.Builder termBuilder = TermWeight.Info.newBuilder();
                termBuilder.addTermOffset(offset);
                Value tfValue = new Value(termBuilder.build().toByteArray());
                context.write(EasyMock.eq(tfBik), EasyMock.eq(tfValue));
                offset += 1;
            }
        }

        return event;
    }

    private Key expectIndexed(Mapper.Context context, String date, String uid, String dataType, String field, String value, boolean writeEvent)
                    throws ParseException, IOException, InterruptedException {
        return expectIndexed(context, date, uid, dataType, field, value, writeEvent, "");
    }

    private Key expectIndexed(Mapper.Context context, String date, String uid, String dataType, String field, String value, boolean writeEvent, String vis)
                    throws ParseException, IOException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse(date);
        long eventTime = getTimestamp(d);
        String shard = getShard(d, uid);

        Key event = new Key(shard, dataType + '\u0000' + uid, field + '\u0000' + value, vis, eventTime);

        if (field.indexOf(".") > -1) {
            field = field.replaceAll("\\..*", "");
        }

        // write a global index key
        Key indexKey = new Key(value, field, shard + '\u0000' + dataType, vis, eventTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        context.write(EasyMock.eq(bik), EasyMock.isA(Value.class));

        // write an fi key
        Key fiKey = new Key(shard, FI_START + field, value + '\u0000' + dataType + '\u0000' + uid, vis, eventTime);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);
        context.write(EasyMock.eq(fiBik), EasyMock.isA(Value.class));

        // write the event key
        if (writeEvent) {
            BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event);
            context.write(EasyMock.eq(eventBik), EasyMock.isA(Value.class));
        }

        return event;
    }

    /**
     * Simple stub to generate a valid shardId for a given date and uid pair
     *
     * @param d
     * @param uid
     * @return the shardId that would be generated for the given date and uid given the conf
     */
    private String getShard(Date d, String uid) {
        RawRecordContainer event = new RawRecordContainerImpl();
        event.setDate(d.getTime());
        event.setId(HashUID.parse(uid));

        return shardIdFactory.getShardId(event);
    }

    private long getTimestamp(Date d) {
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        return c.getTimeInMillis();
    }

    private void enableEventProcessing(boolean exportShard) {
        // ShardReindexMapper.REPROCESS_EVENTS, ShardReindexMapper.DEFAULT_DATA_TYPE, ShardReindexMapper.DATA_TYPE_HANDLER must be set to in order to use
        // ShardReindexMapper.EXPORT_SHARD
        conf.setBoolean(ShardReindexMapper.REPROCESS_EVENTS, true);
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        conf.set(ShardReindexMapper.DATA_TYPE_HANDLER, "datawave.ingest.mapreduce.job.reindex.ShardReindexMapperTest$SimpleShardedHandler");
        conf.setBoolean(ShardReindexMapper.EXPORT_SHARD, exportShard);

    }

    private static class SimpleShardedHandler extends ContentIndexingColumnBasedHandler {
        public SimpleShardedHandler() {}

        @Override
        public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
            return (CSVIngestHelper) helper;
        }
    }
}
