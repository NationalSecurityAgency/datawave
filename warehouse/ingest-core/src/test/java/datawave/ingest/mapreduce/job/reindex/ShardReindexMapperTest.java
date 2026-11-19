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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.shaded.com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;

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
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.protobuf.TermWeight;
import datawave.iterators.FrequencyMetadataAggregator;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.model.DateFrequencyMap;
import datawave.util.accumulo.RFileUtil;

public class ShardReindexMapperTest {

    private final Map<List<?>,Integer> expectedCallCounts = new HashMap<>();
    private final List<Executable> interactionChecks = new ArrayList<>();
    private final Deque<Iterator<Map.Entry<Key,Value>>> scannerResults = new ArrayDeque<>();
    private Configuration conf;
    private ShardIdFactory shardIdFactory;

    private CollectingContextWriter contextWriter;
    private final List<Map.Entry<BulkIngestKey,Value>> expectedRecords = new ArrayList<>();
    private final Map<BulkIngestKey,Integer> expectedKeys = new HashMap<>();
    private int expectedCleanupCalls;
    private boolean outputsComparedWithFiles;
    private Mapper.Context context;

    private AccumuloClient mockClient;
    private SecurityOperations mockSecOps;
    private Scanner mockScanner;

    private final ShardReindexMapper mapper = new ShardReindexMapper();

    @BeforeEach
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

        TypeRegistry.reset();
        setupDataType("samplecsv", "FIELDA,FIELDB,FIELDC,FIELDE,FIELDE_TOKEN,FIELDF,FIELDF_TOKEN,FIELDG,FIELDG_TOKEN", "FIELDB,FIELDD", "FIELDE,FIELDE_TOKEN",
                        "FIELDE,FIELDF,FIELDG");

        shardIdFactory = new ShardIdFactory(conf);

        // disable timestamp
        conf.setBoolean(ShardReindexMapper.FLOOR_TIMESTAMPS, false);

        // Collect output while retaining mocks for the Hadoop context and scanner boundary.
        contextWriter = new CollectingContextWriter();
        context = mock(Mapper.Context.class);
        mockClient = mock(AccumuloClient.class);
        mockSecOps = mock(SecurityOperations.class);
        mockScanner = mock(Scanner.class);
    }

    @AfterEach
    public void verifyInteractionsAndOutput() {
        assertAll(interactionChecks);
        if (!outputsComparedWithFiles) {
            assertWrittenRecords();
        }
        assertEquals(expectedCleanupCalls, contextWriter.cleanupCalls);
        verifyNoMoreInteractions(ignoreStubs(context, mockClient, mockScanner, mockSecOps));
    }

    private void setupDataType(String name, String indexed, String reverseIndexed, String indexOnly, String tokenized) {
        // simple required config for a type with some indexed fields
        conf.set(name + DATA_HEADER, "a,b,c,d,e");
        conf.set(name + DATA_SEP, ",");
        conf.set(name + INDEX_FIELDS, indexed);
        conf.set(name + REVERSE_INDEX_FIELDS, reverseIndexed);
        conf.set(name + INDEX_ONLY_FIELDS, indexOnly);
        conf.set(name + TOKEN_INDEX_ALLOWLIST, tokenized);

        Type t = new Type(name, CSVIngestHelper.class, null, null, 0, null);
        // this needs to be called each test to clear any static config that may be cached
        t.clearIngestHelper();

        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(t.typeName(), t);
    }

    @Test
    public void FI_unindexed_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void FI_unindexed_export_test() throws IOException, InterruptedException {
        conf.setBoolean(ShardReindexMapper.EXPORT_SHARD, true);

        lenient().doReturn(conf).when(context).getConfiguration();

        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void FI_unindexedCleanup_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELD", "VALUE" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key deletedkey = new Key(fiKey);
        deletedkey.setDeleted(true);
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), deletedkey);

        lenient().doReturn(conf).when(context).getConfiguration();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_forwardIndex_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);

        lenient().doReturn(conf).when(context).getConfiguration();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_forwardIndex_export_notIndexOnly_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void FI_forwardIndex_export_indexOnly_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDE", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDE", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        // write both the shardIndex and the original fi

        conf.setBoolean(ShardReindexMapper.ENABLE_REINDEX_COUNTERS, true);
        conf.setBoolean(ShardReindexMapper.DUMP_COUNTERS, true);

        mapper.setup(context);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).write(eq(bik), isA(Value.class));
        verify(context, times(1)).write(eq(fiBik), isA(Value.class));
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_forwardAndReverseIndex_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDB", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDB", "row" + '\u0000' + "samplecsv");
        Key revKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");

        BulkIngestKey bik1 = new BulkIngestKey(new Text("shardIndex"), indexKey);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        lenient().doReturn(conf).when(context).getConfiguration();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);

        expectWrittenKey(bik1);
        expectWrittenKey(bik2);
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_reverseIndex_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv");
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        lenient().doReturn(conf).when(context).getConfiguration();

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);

        expectWrittenKey(bik2);
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_deletedKey_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        fiKey.setDeleted(true);
        Key revKey = new Key("CBA", "FIELDD", "row" + '\u0000' + "samplecsv");
        revKey.setDeleted(true);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("shardReverseIndex"), revKey);

        lenient().doReturn(conf).when(context).getConfiguration();

        conf.setBoolean(ShardReindexMapper.FLOOR_TIMESTAMPS, false);
        conf.setBoolean(ShardReindexMapper.PROPAGATE_DELETES, true);

        // enable cleanup keys
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);

        expectWrittenKey(bik2);
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_deletedKey_noPropagate_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDD", "ABC" + '\u0000' + "samplecsv" + '\u0000' + "1.2.3");
        fiKey.setDeleted(true);

        lenient().doReturn(conf).when(context).getConfiguration();

        conf.setBoolean(ShardReindexMapper.PROPAGATE_DELETES, false);
        conf.setBoolean(ShardReindexMapper.CLEANUP_SHARD, true);

        mapper.setup(context);

        mapper.map(fiKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void E_notProcessing_test() throws IOException, InterruptedException {
        Key event = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDB" + '\u0000' + "my field b value", 1000l);

        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        mapper.map(event, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void E_unindexed_test() throws IOException, InterruptedException, ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELD_UNINDEXED" + '\u0000' + "my field b value", eventTime);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        // output the event only
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), event);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void E_forwardIndex_test() throws IOException, InterruptedException, ParseException {
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA", "ABC", true);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void E_forwardIndex_groupingNotation_test() throws IOException, InterruptedException, ParseException {
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA.123.234.345.456", "ABC", true);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void E_forwardIndexTokenized_noTF_test() throws IOException, InterruptedException, ParseException {
        lenient().doNothing().when(context).progress();
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "generate some tokens", new String[] {"generate", "some", "tokens"},
                        false);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

    }

    @Test
    public void E_forwardIndexTokenized_test() throws IOException, InterruptedException, ParseException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "generate some tokens", new String[] {"generate", "some", "tokens"},
                        true);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

    }

    // all offsets are at 1 instead of being processed together
    @Test
    public void E_batchNone_multiValueToken_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);

    }

    // multivalued tokens are offset by being processed together but no cleanup called
    @Test
    public void E_batchField_multiValueToken_noCleanup_test() throws ParseException, IOException, InterruptedException {
        enableEventProcessing(true);
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        lenient().doReturn(conf).when(context).getConfiguration();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event1 = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDF" + '\u0000' + "value1", eventTime);
        Key event2 = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDF" + '\u0000' + "value2", eventTime);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);

        expectWrittenKey(new BulkIngestKey(new Text("shard"), event1));
        expectWrittenKey(new BulkIngestKey(new Text("shard"), event2));
        verify(context, times(2)).progress();

    }

    @Test
    public void E_batchField_multiValueToken_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 11);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0);
        Key event3 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value3", new String[] {"value3"}, true, 22);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.map(event3, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchField_multiValueToken_mixedVis_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 11, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");
        Key event3 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value3", new String[] {"value3"}, true, 0, "b");

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.map(event3, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchField_mixedEvent_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchField_tokenizedBatch_backToBack_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDG", "value1", new String[] {"value1"}, true);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchEvent_tokenizedBatch_backToBack_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDG", "value1", new String[] {"value1"}, true);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchEvent_mixedEvent_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchEvent_longMixedEvent_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3.4.6", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4.2.1", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchEvent_mixedEvents_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.EVENT.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true, 0, "a");
        Key event1a = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value3", new String[] {"value3"}, true, 11, "a");
        Key event2 = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value2", new String[] {"value2"}, true, 0, "a");
        Key event2a = expectTokenized(context, "20240216", "1.2.4", "samplecsv", "FIELDF", "value4", new String[] {"value4"}, true, 11, "a");

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event1a, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.map(event2a, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_batchField_tokenizedBatch_followingKey_test() throws ParseException, IOException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        conf.set(ShardReindexMapper.BATCH_MODE, ShardReindexMapper.BatchMode.FIELD.name());
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        // expect all keys for the tokenized field
        Key event1 = expectTokenized(context, "20240216", "1.2.3", "samplecsv", "FIELDF", "value1", new String[] {"value1"}, true);
        Key event2 = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA", "not tokenized", true);

        // ingest handler may call progress internally

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event1, new Value(), context);
        mapper.map(event2, new Value(), context);
        mapper.cleanup(context);

        expectedCleanupCalls = 1;

    }

    @Test
    public void E_reverseIndex_test() throws IOException, InterruptedException, ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDD" + '\u0000' + "ABC", eventTime);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        // write a reverse global index key
        Key indexKey = new Key("CBA", "FIELDD", shard + '\u0000' + "samplecsv", eventTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardReverseIndex"), indexKey);

        // DO NOT write an fi key (turns out this isn't expected)

        // write the event key
        BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

        expectWrittenKey(bik);
        expectWrittenKey(eventBik);
        verify(context, times(1)).progress();

    }

    @Test
    public void E_forwardAndReverseIndex_test() throws IOException, InterruptedException, ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d = sdf.parse("20240216");
        long eventTime = getTimestamp(d);
        String uid = "1.2.3";
        String shard = getShard(d, uid);

        Key event = new Key(shard, "samplecsv" + '\u0000' + uid, "FIELDB" + '\u0000' + "this could be tokenized", eventTime);

        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        // write a global index key
        Key indexKey = new Key("this could be tokenized", "FIELDB", shard + '\u0000' + "samplecsv", eventTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);

        // write a reverse global index key
        Key rindexKey = new Key("dezinekot eb dluoc siht", "FIELDB", shard + '\u0000' + "samplecsv", eventTime);
        BulkIngestKey rbik = new BulkIngestKey(new Text("shardReverseIndex"), rindexKey);

        // write an fi key
        Key fiKey = new Key(shard, FI_START + "FIELDB", "this could be tokenized" + '\u0000' + "samplecsv" + '\u0000' + uid, eventTime);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);

        // write the event key
        BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);

        expectWrittenKey(bik);
        expectWrittenKey(rbik);
        expectWrittenKey(fiBik);
        expectWrittenKey(eventBik);
        verify(context, times(1)).progress();

    }

    @Test
    public void FI_unknownDataType_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "someUnknownDataType" + '\u0000' + "1.2.3");

        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        assertThrows(IllegalStateException.class, () -> mapper.map(fiKey, new Value(), context));
    }

    // unlike unknownDataTypeFI_test() this uses the default data type to provide processing capability
    @Test
    public void FI_unknownDataType_wDefaultHelper_test() throws IOException, InterruptedException {
        Key fiKey = new Key("row", FI_START + "FIELDA", "ABC" + '\u0000' + "someUnknownDataType" + '\u0000' + "1.2.3");
        Key indexKey = new Key("ABC", "FIELDA", "row" + '\u0000' + "someUnknownDataType");
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);

        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");

        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void TF_noReprocess_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void TF_reprocess_notExported_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(false);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void TF_reprocess_indexOnlyNotGenerateTF_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), tfKey);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(tfKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void TF_reprocess_indexOnlyGenerateTF_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDE");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), tfKey);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(tfKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void TF_reprocess_notIndexOnlyNotGenerateTF_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDF");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), tfKey);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(tfKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void TF_reprocess_notIndexOnlyGenerateTF_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        conf.setBoolean(ShardReindexMapper.GENERATE_TF, true);
        enableEventProcessing(true);

        Key tfKey = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "abc" + '\u0000' + "FIELDF");

        mapper.setup(context);
        mapper.map(tfKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void D_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");

        Key dKey = new Key("row", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someViewName");

        mapper.setup(context);
        mapper.map(dKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void D_eventProcess_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(false);

        Key dKey = new Key("row", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someViewName");

        mapper.setup(context);
        mapper.map(dKey, new Value(), context);

        verify(context, times(1)).progress();

    }

    @Test
    public void D_eventProcessExport_test() throws IOException, InterruptedException {
        lenient().doReturn(conf).when(context).getConfiguration();
        // set this as the default
        conf.set(ShardReindexMapper.DEFAULT_DATA_TYPE, "samplecsv");
        enableEventProcessing(true);

        Key dKey = new Key("row", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someViewName");
        BulkIngestKey bik = new BulkIngestKey(new Text("shard"), dKey);

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(dKey, new Value(), context);

        expectWrittenKey(bik);
        verify(context, times(1)).progress();

    }

    @Test
    public void E_forwardIndex_metadata_test() throws IOException, InterruptedException, ParseException {
        conf.setBoolean(ShardReindexMapper.GENERATE_METADATA, true);
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Key event = expectIndexed(context, "20240216", "1.2.3", "samplecsv", "FIELDA", "ABC", true);

        expectMetadata("FIELDA", "f", "samplecsv", "20240216", 1L, event.getTimestamp());
        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1L, event.getTimestamp());
        expectMetadata("FIELDA", "e", "samplecsv", null, event.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), event.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(event, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    private void configureMetadataOnly(boolean metadataOnly, boolean generateFromFi, boolean disableFrequencyCounts, boolean generateRiFromFi,
                    boolean lookupRiFromFi, boolean generateEventFromFi, boolean lookupEventFromFi) {
        conf.setBoolean(ShardReindexMapper.METADATA_ONLY, metadataOnly);
        conf.setBoolean(ShardReindexMapper.METADATA_GENERATE_FROM_FI, generateFromFi);
        conf.setBoolean(ShardReindexMapper.METADATA_DISABLE_FREQUENCY_COUNTS, disableFrequencyCounts);
        conf.setBoolean(ShardReindexMapper.METADATA_GENERATE_RI_FROM_FI, generateRiFromFi);
        conf.setBoolean(ShardReindexMapper.LOOKUP_RI_METADATA_FROM_FI, lookupRiFromFi);
        conf.setBoolean(ShardReindexMapper.METADATA_GENERATE_EVENT_FROM_FI, generateEventFromFi);
        conf.setBoolean(ShardReindexMapper.LOOKUP_EVENT_METADATA_FROM_FI, lookupEventFromFi);
    }

    private void expectMetadata(String field, String type, String dataType, String value, long timestamp) throws IOException, InterruptedException {
        Key iKey;
        if (value != null) {
            iKey = new Key(field, type, dataType + '\u0000' + value, timestamp);
        } else {
            iKey = new Key(field, type, dataType, timestamp);
        }
        BulkIngestKey iBik = new BulkIngestKey(new Text("DatawaveMetadata"), iKey);
        expectWrittenKey(iBik);
    }

    private void expectMetadata(String field, String type, String dataType, String date, long count, long timestamp) throws IOException, InterruptedException {
        Key iKey;
        iKey = new Key(field, type, dataType + '\u0000' + FrequencyMetadataAggregator.AGGREGATED, timestamp);
        BulkIngestKey iBik = new BulkIngestKey(new Text("DatawaveMetadata"), iKey);
        DateFrequencyMap map = new DateFrequencyMap();
        map.put(date, count);
        expectedRecords.add(Map.entry(iBik, new Value(map.toBytes())));
    }

    private Key createFiKey(String row, String field, String value, String dataType, String uid, String date) throws ParseException {
        Key fiKey = new Key(row, FI_START + field, value + '\u0000' + dataType + '\u0000' + uid);

        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Date d = sdf.parse(date);
            long eventTime = getTimestamp(d);
            fiKey.setTimestamp(eventTime);
        }

        return fiKey;
    }

    private void expectScanner(String tableName, Range r, Iterator<Map.Entry<Key,Value>> result)
                    throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        doReturn("testUser").when(mockClient).whoami();
        List<?> expectedCall2 = Arrays.asList(mockClient, "whoami");
        expectedCallCounts.merge(expectedCall2, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockClient, times(expectedCallCounts.get(expectedCall2))).whoami());
        doReturn(mockSecOps).when(mockClient).securityOperations();
        List<?> expectedCall3 = Arrays.asList(mockClient, "securityOperations");
        expectedCallCounts.merge(expectedCall3, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockClient, times(expectedCallCounts.get(expectedCall3))).securityOperations());
        doReturn(new Authorizations()).when(mockSecOps).getUserAuthorizations("testUser");
        List<?> expectedCall4 = Arrays.asList(mockSecOps, "getUserAuthorizations", "testUser");
        expectedCallCounts.merge(expectedCall4, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockSecOps, times(expectedCallCounts.get(expectedCall4))).getUserAuthorizations("testUser"));
        doReturn(mockScanner).when(mockClient).createScanner(eq(tableName), isA(Authorizations.class));
        List<?> expectedCall5 = Arrays.asList(mockClient, "createScanner", tableName, Authorizations.class);
        expectedCallCounts.merge(expectedCall5, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockClient, times(expectedCallCounts.get(expectedCall5))).createScanner(eq(tableName), isA(Authorizations.class)));

        scannerResults.add(result);
        doAnswer(invocation -> scannerResults.removeFirst()).when(mockScanner).iterator();
        List<?> expectedCall6 = Arrays.asList(mockScanner, "iterator");
        expectedCallCounts.merge(expectedCall6, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockScanner, times(expectedCallCounts.get(expectedCall6))).iterator());
        List<?> expectedCall7 = Arrays.asList(mockScanner, "setRange", r);
        expectedCallCounts.merge(expectedCall7, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockScanner, times(expectedCallCounts.get(expectedCall7))).setRange(eq(r)));
        List<?> expectedCall8 = Arrays.asList(mockScanner, "close");
        expectedCallCounts.merge(expectedCall8, 1, Integer::sum);
        interactionChecks.add(() -> verify(mockScanner, times(expectedCallCounts.get(expectedCall8))).close());
    }

    @Test
    public void FI_metadataOnly_deletedKey_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDD", "ABC", "samplecsv", "1.2.3", null);
        fiKey.setDeleted(true);

        configureMetadataOnly(true, true, false, false, false, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_unindexedKey_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDZ", "ABC", "samplecsv", "1.2.3", null);

        configureMetadataOnly(true, true, false, false, false, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_unindexedKeyWithCountsAndEvents_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDZ", "ABC", "samplecsv", "1.2.3", null);

        configureMetadataOnly(true, true, false, false, false, true, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexed_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, false, false, false, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        // TODO this might be a bug in EventMetadata, but the frequency key comes with the event key, so is excluded here
        // expectMetadata("FIELDA", "f", "samplecsv", "20240216", fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexedEventNoFrequency_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexedEventAndFrequency_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, false, false, false, true, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDA", "f", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexedEventLookupFail_test()
                    throws IOException, InterruptedException, ParseException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        setupDataType("samplecsv2", "FIELDA", "FIELDD", "FIELDE,FIELDE_TOKEN", "FIELDE,FIELDF,FIELDG");

        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");
        Key nextFiKey = createFiKey("row", "FIELDA", "DEF", "samplecsv", "1.2.3", "20240216");
        Key fiKeyDataType2 = createFiKey("row", "FIELDA", "DEF", "samplecsv2", "1.2.3", "20240216");

        configureMetadataOnly(true, true, false, false, false, true, true);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 2L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key eventLookupKey = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDA" + '\u0000');
        Key eventLookupKeyEnd = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDA" + '.' + '\uFFFF');
        Range expectedLookupRange = new Range(eventLookupKey, true, eventLookupKeyEnd, true);
        expectScanner("shard", expectedLookupRange, Collections.emptyIterator());

        expectMetadata("FIELDA", "i", "samplecsv2", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv2", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        eventLookupKey = new Key("row", "samplecsv2" + '\u0000' + "1.2.3", "FIELDA" + '\u0000');
        eventLookupKeyEnd = new Key("row", "samplecsv2" + '\u0000' + "1.2.3", "FIELDA" + '.' + '\uFFFF');
        expectedLookupRange = new Range(eventLookupKey, true, eventLookupKeyEnd, true);
        expectScanner("shard", expectedLookupRange, Collections.emptyIterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        // this causes no recheck because the previous combo of FIELDA/samplecsv
        mapper.map(nextFiKey, new Value(), context);
        // this will have an event check because the FIELDA/samplecsv2 combo hasn't been seen
        mapper.map(fiKeyDataType2, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(3)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexedEventLookup_test()
                    throws IOException, InterruptedException, ParseException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, false, false, false, true, true);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDA", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDA", "f", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key eventLookupKey = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDA" + '\u0000');
        Key eventLookupKeyEnd = new Key("row", "samplecsv" + '\u0000' + "1.2.3", "FIELDA" + '.' + '\uFFFF');
        Range expectedLookupRange = new Range(eventLookupKey, true, eventLookupKeyEnd, true);
        expectScanner("shard", expectedLookupRange, List.of(Map.entry(new Key(), new Value())).iterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexedNoFrequency_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_indexedWithTfNoLookup_test() throws IOException, InterruptedException, ParseException {
        // define a new dataType which does not use a token designator
        setupDataType("undesignatedTokens", "FIELDA,FIELDB", "", "FIELDB", "FIELDA,FIELDB");
        conf.setBoolean("undesignatedTokens.data.category.token.fieldname.designator.enabled", false);

        // this field is indexed and index only, but was the result of tokenization
        Key fiKey = createFiKey("row", "FIELDE_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");
        // this field is indexed, tokenized, and index only
        Key nonTokenFi = createFiKey("row", "FIELDE", "ABC", "samplecsv", "1.2.3", "20240216");

        // this field is marked as indexed, but was the result of tokenization
        Key regularTokenizedFi = createFiKey("row", "FIELDF_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");
        // this field is marked as indexed and tokenized
        Key indexedFiThatWasTokenized = createFiKey("row", "FIELDF", "ABC", "samplecsv", "1.2.3", "20240216");

        // use the new data type to test that undesignated tokens also generate the correct metadata
        Key undesignatedFi1 = createFiKey("row", "FIELDA", "ABC", "undesignatedTokens", "1.2.3", "20240216");
        Key undesignatedFi2 = createFiKey("row", "FIELDB", "ABC", "undesignatedTokens", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        conf.setBoolean(ShardReindexMapper.METADATA_GENEREATE_TF_FROM_FI, true);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDE_TOKEN", "i", "samplecsv", "20240216", 2, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "tf", "samplecsv", null, fiKey.getTimestamp());

        expectMetadata("FIELDE", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDE", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDF_TOKEN", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "i", "samplecsv", "20240216", 2, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "tf", "samplecsv", null, fiKey.getTimestamp());

        expectMetadata("FIELDF", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDF", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDA", "e", "undesignatedTokens", null, fiKey.getTimestamp());
        expectMetadata("FIELDA", "i", "undesignatedTokens", "20240216", 2, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "undesignatedTokens", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());
        expectMetadata("FIELDA", "tf", "undesignatedTokens", null, fiKey.getTimestamp());

        expectMetadata("FIELDB", "i", "undesignatedTokens", "20240216", 2, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "undesignatedTokens", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());
        expectMetadata("FIELDB", "tf", "undesignatedTokens", null, fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(nonTokenFi, new Value(), context);
        mapper.map(regularTokenizedFi, new Value(), context);
        mapper.map(indexedFiThatWasTokenized, new Value(), context);
        mapper.map(undesignatedFi1, new Value(), context);
        mapper.map(undesignatedFi2, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(6)).progress();
        expectedCleanupCalls = 1;

    }

    // this test verifies the RegExFilter applied for TF lookups works as expected
    @Test
    public void verifyRegExFilterWorksTest() throws IOException {
        List<Map.Entry<Key,Value>> data = new ArrayList();
        data.add(Map.entry(new Key("a", "b", "c"), new Value()));
        data.add(Map.entry(new Key("a", "b", "cc"), new Value()));
        data.add(Map.entry(new Key("a", "b", "c" + '\u0000' + "df"), new Value()));
        data.add(Map.entry(new Key("a", "b", "c" + '\u0000' + "DF"), new Value()));
        data.add(Map.entry(new Key("a", "b", "c" + '\u0000' + " df"), new Value()));
        SortedListKeyValueIterator iterator = new SortedListKeyValueIterator(data);
        RegExFilter filter = new RegExFilter();
        Map<String,String> options = new HashMap<>();
        options.put("colqRegex", '\u0000' + "df$");
        options.put("matchSubstring", "true");

        filter.init(iterator, options, null);
        filter.seek(new Range(new Key("a"), null), Collections.emptySet(), false);

        assertTrue(filter.hasTop());
        int count = 0;
        while (filter.hasTop()) {
            count++;
            filter.next();
        }

        assertEquals(1, count);
    }

    @Test
    public void FI_metadataOnly_unrestrictedIngestHelperTf_test()
                    throws ParseException, IOException, InterruptedException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        Key fiKey = createFiKey("row", "FIELDE_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");
        configureMetadataOnly(true, true, true, true, false, true, false);
        conf.setBoolean(ShardReindexMapper.METADATA_GENEREATE_TF_FROM_FI, true);
        conf.setBoolean(ShardReindexMapper.LOOKUP_TF_METADATA_FROM_FI, true);

        lenient().doReturn(conf).when(context).getConfiguration();

        Key start = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000');
        Key end = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0001');
        Range r = new Range(start, true, end, false);
        expectScanner("shard", r, List.of(Map.entry(new Key(), new Value())).iterator());
        ArgumentCaptor<IteratorSetting> fieldECapture = ArgumentCaptor.forClass(IteratorSetting.class);

        expectMetadata("FIELDE_TOKEN", "i", "samplecsv", "20240216", 2L, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "tf", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;
        verify(mockScanner).addScanIterator(fieldECapture.capture());

    }

    @Test
    public void FI_metadataOnly_tfLookupFail_test()
                    throws ParseException, IOException, InterruptedException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        Key fiKey = createFiKey("row", "FIELDE_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        conf.setBoolean(ShardReindexMapper.METADATA_GENEREATE_TF_FROM_FI, true);
        conf.setBoolean(ShardReindexMapper.LOOKUP_TF_METADATA_FROM_FI, true);

        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDE_TOKEN", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key start = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000');
        Key end = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0001');
        Range r = new Range(start, true, end, false);
        expectScanner("shard", r, Collections.emptyIterator());
        ArgumentCaptor<IteratorSetting> iteratorSettingCapture = ArgumentCaptor.forClass(IteratorSetting.class);

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;
        verify(mockScanner, times(1)).addScanIterator(iteratorSettingCapture.capture());

        // make sure the regex iterator on the scanner is setup correctly
        assertEquals('\u0000' + "FIELDE_TOKEN$", iteratorSettingCapture.getValue().getOptions().get("colqRegex"));
        assertEquals("true", iteratorSettingCapture.getValue().getOptions().get("matchSubstring"));

    }

    @Test
    public void FI_metadataOnly_tfLookupCache_test()
                    throws ParseException, IOException, InterruptedException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        Key fiKey = createFiKey("row", "FIELDE_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");
        Key nextFiKey = createFiKey("row", "FIELDE_TOKEN", "DEF", "samplecsv", "1.2.3", "20240216");
        Key fFiKey = createFiKey("row", "FIELDF_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        conf.setBoolean(ShardReindexMapper.METADATA_GENEREATE_TF_FROM_FI, true);
        conf.setBoolean(ShardReindexMapper.LOOKUP_TF_METADATA_FROM_FI, true);

        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDE_TOKEN", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDF_TOKEN", "i", "samplecsv", "20240216", 2, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "tf", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        // fieldE scanner
        Key start = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000');
        Key end = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0001');
        Range r = new Range(start, true, end, false);
        expectScanner("shard", r, Collections.emptyIterator());
        ArgumentCaptor<IteratorSetting> fieldECapture = ArgumentCaptor.forClass(IteratorSetting.class);

        // fieldF scanner
        start = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0000');
        end = new Key("row", "tf", "samplecsv" + '\u0000' + "1.2.3" + '\u0001');
        r = new Range(start, true, end, false);
        expectScanner("shard", r, List.of(Map.entry(new Key(), new Value())).iterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(nextFiKey, new Value(), context);
        mapper.map(fFiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(3)).progress();
        expectedCleanupCalls = 1;
        verify(mockScanner, times(2)).addScanIterator(fieldECapture.capture());

        // make sure the regex iterator on the scanner is setup correctly
        assertEquals('\u0000' + "FIELDE_TOKEN$", fieldECapture.getAllValues().get(0).getOptions().get("colqRegex"));
        assertEquals("true", fieldECapture.getAllValues().get(0).getOptions().get("matchSubstring"));
        assertEquals('\u0000' + "FIELDF_TOKEN$", fieldECapture.getAllValues().get(1).getOptions().get("colqRegex"));

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedAssumeReverse_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, true, false, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "ri", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedAssumeReverseWithEvent_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, true, false, true, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "ri", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedNoReverse_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedNoReverseWithEvent_test() throws IOException, InterruptedException, ParseException {
        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedVerifyReverseFail_test()
                    throws IOException, InterruptedException, ParseException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, true, true, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key riLookupKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");
        Range expectedLookupRange = new Range(riLookupKey, true, riLookupKey, true);
        expectScanner("shardReverseIndex", expectedLookupRange, Collections.emptyIterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(1)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedVerifyReverse_test()
                    throws IOException, InterruptedException, ParseException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");
        // this key will not have a scanner created because this field/datatype has already been validated and no counts are being generated
        Key anotherFiKey = createFiKey("row", "FIELDB", "XYZ", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, true, true, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "ri", "samplecsv", "20240216", 1, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key riLookupKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");
        Range expectedLookupRange = new Range(riLookupKey, true, riLookupKey, true);
        expectScanner("shardReverseIndex", expectedLookupRange, List.of(Map.entry(new Key(), new Value())).iterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(anotherFiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(2)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_forwardAndReverseIndexedVerifyReverseCacheVerify_test()
                    throws IOException, InterruptedException, ParseException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        setupDataType("samplecsv2", "FIELDB", "FIELDD", "FIELDE,FIELDE_TOKEN", "FIELDE,FIELDF,FIELDG");

        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");
        Key altFiKey = createFiKey("row", "FIELDB", "DEF", "samplecsv", "1.2.3", "20240216");
        Key sample2FiKey = createFiKey("row", "FIELDB", "DEF", "samplecsv2", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, true, true, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "ri", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDB", "i", "samplecsv2", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv2", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key riLookupKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");
        Range expectedLookupRange = new Range(riLookupKey, true, riLookupKey, true);
        expectScanner("shardReverseIndex", expectedLookupRange, List.of(Map.entry(new Key(), new Value())).iterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(altFiKey, new Value(), context);
        mapper.map(sample2FiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(3)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_dataTypeVerify_test()
                    throws IOException, InterruptedException, ParseException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        setupDataType("samplecsv2", "FIELDB", "FIELDD", "FIELDE,FIELDE_TOKEN", "FIELDE,FIELDF,FIELDG");

        Key fiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");
        Key sample2FiKey = createFiKey("row", "FIELDB", "ABC", "samplecsv2", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, true, true, false, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "ri", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDB", "i", "samplecsv2", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv2", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        Key riLookupKey = new Key("CBA", "FIELDB", "row" + '\u0000' + "samplecsv");
        Range expectedLookupRange = new Range(riLookupKey, true, riLookupKey, true);
        expectScanner("shardReverseIndex", expectedLookupRange, List.of(Map.entry(new Key(), new Value())).iterator());

        mapper.setAccumuloClient(mockClient);
        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(sample2FiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(2)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_disableFrequency_test() throws ParseException, IOException, InterruptedException {
        setupDataType("samplecsv2", "FIELDA,FIELDB", "", "FIELDB", "");
        Key fiKey = createFiKey("row", "FIELDA", "ABC", "samplecsv", "1.2.3", "20240216");
        Key dupDataType = createFiKey("row", "FIELDA", "DEF", "samplecsv", "1.2.3", "20240216");
        Key nextDataType = createFiKey("row", "FIELDA", "ABC", "samplecsv2", "1.2.3", "20240216");
        Key nextField = createFiKey("row", "FIELDB", "ABC", "samplecsv", "1.2.3", "20240216");
        Key nextFieldDataType = createFiKey("row", "FIELDB", "DEF", "samplecsv2", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDA", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDA", "i", "samplecsv2", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDA", "e", "samplecsv2", null, fiKey.getTimestamp());
        expectMetadata("FIELDA", "t", "samplecsv2", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDB", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDB", "i", "samplecsv2", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDB", "t", "samplecsv2", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(dupDataType, new Value(), context);
        mapper.map(nextDataType, new Value(), context);
        mapper.map(nextField, new Value(), context);
        mapper.map(nextFieldDataType, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(5)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void FI_metadataOnly_generateTf_test() throws ParseException, IOException, InterruptedException {
        Key fiKey = createFiKey("row", "FIELDE_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");
        Key nextFiKey = createFiKey("row", "FIELDE_TOKEN", "DEF", "samplecsv", "1.2.3", "20240216");
        Key fFiKey = createFiKey("row", "FIELDF_TOKEN", "ABC", "samplecsv", "1.2.3", "20240216");
        Key nonTokenFiKey = createFiKey("row", "FIELDF", "ABC", "samplecsv", "1.2.3", "20240216");

        configureMetadataOnly(true, true, true, false, false, true, false);
        conf.setBoolean(ShardReindexMapper.METADATA_GENEREATE_TF_FROM_FI, true);
        conf.setBoolean(ShardReindexMapper.LOOKUP_TF_METADATA_FROM_FI, false);

        lenient().doReturn(conf).when(context).getConfiguration();

        expectMetadata("FIELDE_TOKEN", "i", "samplecsv", "20240216", 2L, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "tf", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDE_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDF_TOKEN", "i", "samplecsv", "20240216", 2L, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "tf", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF_TOKEN", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        expectMetadata("FIELDF", "i", "samplecsv", "20240216", 1L, fiKey.getTimestamp());
        expectMetadata("FIELDF", "e", "samplecsv", null, fiKey.getTimestamp());
        expectMetadata("FIELDF", "t", "samplecsv", NoOpType.class.getCanonicalName(), fiKey.getTimestamp());

        mapper.setup(context);
        mapper.setContextWriter(contextWriter);
        mapper.map(fiKey, new Value(), context);
        mapper.map(nextFiKey, new Value(), context);
        mapper.map(fFiKey, new Value(), context);
        mapper.map(nonTokenFiKey, new Value(), context);
        mapper.cleanup(context);

        verify(context, times(4)).progress();
        expectedCleanupCalls = 1;

    }

    @Test
    public void createAndVerifyTest() throws IOException, ClassNotFoundException, InterruptedException {
        lenient().doNothing().when(context).progress();
        conf.setBoolean(ShardReindexMapper.ENABLE_REINDEX_COUNTERS, true);
        conf.setBoolean(ShardReindexMapper.DUMP_COUNTERS, true);
        conf.setBoolean(ShardReindexMapper.GENERATE_METADATA, true);
        enableEventProcessing(true);
        lenient().doReturn(conf).when(context).getConfiguration();

        Multimap generated = TreeMultimap.create();

        // ingest handler may access counters, this is a catch all
        lenient().doReturn(new Counters.Counter()).when(context).getCounter(isA(String.class), isA(String.class));

        // other context for the verification mapper
        Mapper.Context verificationContext = mock(Mapper.Context.class);
        lenient().doNothing().when(verificationContext).progress();
        lenient().doReturn(new Counters.Counter()).when(verificationContext).getCounter(isA(String.class), isA(String.class));

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
            mapper.setContextWriter(contextWriter);
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
            contextWriter.records.forEach(record -> generated.put(record.getKey(), record.getValue()));

            System.out.println("processing diff");
            outputsComparedWithFiles = true;
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

        expectedCleanupCalls = 1;

        assertEquals(expectedCleanupCalls, contextWriter.cleanupCalls);
        if (verificationContext != null && mockingDetails(verificationContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(verificationContext));
        }
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
                expectedRecords.add(Map.entry(tfBik, tfValue));
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

        if (field.indexOf('.') > -1) {
            field = field.replaceAll("\\..*", "");
        }

        // write a global index key
        Key indexKey = new Key(value, field, shard + '\u0000' + dataType, vis, eventTime);
        BulkIngestKey bik = new BulkIngestKey(new Text("shardIndex"), indexKey);
        expectWrittenKey(bik);

        // write an fi key
        Key fiKey = new Key(shard, FI_START + field, value + '\u0000' + dataType + '\u0000' + uid, vis, eventTime);
        BulkIngestKey fiBik = new BulkIngestKey(new Text("shard"), fiKey);
        expectWrittenKey(fiBik);

        // write the event key
        if (writeEvent) {
            BulkIngestKey eventBik = new BulkIngestKey(new Text("shard"), event);
            expectWrittenKey(eventBik);
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
        event.setTimestamp(d.getTime());
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

    private void expectWrittenKey(BulkIngestKey key) {
        expectedKeys.merge(key, 1, Integer::sum);
    }

    private void assertWrittenRecords() {
        List<Map.Entry<BulkIngestKey,Value>> remaining = new ArrayList<>(contextWriter.records);
        for (Map.Entry<BulkIngestKey,Value> expected : expectedRecords) {
            assertTrue(remaining.remove(expected), "Missing output record: " + expected);
        }
        Map<BulkIngestKey,Integer> actualKeys = new HashMap<>();
        remaining.forEach(record -> actualKeys.merge(record.getKey(), 1, Integer::sum));
        assertEquals(expectedKeys, actualKeys, "Unexpected or missing output records");
    }

    private class CollectingContextWriter implements ContextWriter<BulkIngestKey,Value> {
        private final List<Map.Entry<BulkIngestKey,Value>> records = new ArrayList<>();
        private int cleanupCalls;

        @Override
        public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,BulkIngestKey,Value> writeContext) {
            assertSame(context, writeContext);
            // The mapper may reuse its keys and values after writing them.
            BulkIngestKey copy = new BulkIngestKey(new Text(key.getTableName()), new Key(key.getKey()));
            records.add(Map.entry(copy, new Value(value)));
        }

        @Override
        public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> writeContext) {
            fail("Unexpected bulk write");
        }

        @Override
        public void setup(Configuration configuration, boolean outputTableCounters) {
            fail("Unexpected writer setup");
        }

        @Override
        public void commit(TaskInputOutputContext<?,?,BulkIngestKey,Value> writeContext) {
            fail("Unexpected writer commit");
        }

        @Override
        public void rollback() {
            fail("Unexpected writer rollback");
        }

        @Override
        public void cleanup(TaskInputOutputContext<?,?,BulkIngestKey,Value> writeContext) {
            assertSame(context, writeContext);
            cleanupCalls++;
        }
    }
}
