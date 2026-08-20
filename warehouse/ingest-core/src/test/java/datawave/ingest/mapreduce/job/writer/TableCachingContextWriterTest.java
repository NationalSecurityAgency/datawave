package datawave.ingest.mapreduce.job.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.mapreduce.job.reduce.AggregatingReducer;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner;

public class TableCachingContextWriterTest {

    private static final long TIMESTAMP = 1691931000000L;

    private Configuration conf;
    private TableCachingContextWriter writer;
    private TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        // TableConfigCache is a JVM-wide static singleton; reset it so our conf is actually consulted
        Field cacheField = TableConfigCache.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        cacheField.set(null, null);

        CaptureContextWriter.captured = ArrayListMultimap.create();

        conf = new Configuration(false);

        // AccumuloHelper.setup() only validates these are non-empty; no network call is made on this code path
        AccumuloHelper.setUsername(conf, "root");
        AccumuloHelper.setPassword(conf, "pass".getBytes());
        AccumuloHelper.setInstanceName(conf, "instance");
        AccumuloHelper.setZooKeepers(conf, "localhost:2181");

        conf.setBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, true);
        conf.set(AggregatingReducer.INGEST_VALUE_DEDUP_BY_TIMESTAMP_KEY, "table1");
        conf.setBoolean("table1" + AggregatingReducer.USE_AGGREGATOR_PROPERTY, true);
        conf.setInt("table1" + TableCachingContextWriter.TABLES_TO_CACHE_SUFFIX, 100);
        conf.setClass(TableCachingContextWriter.CONTEXT_WRITER_CLASS, CaptureContextWriter.class, ContextWriter.class);

        TableConfigurationUtil.addOutputTables("table1,table2", conf);

        Map<String,String> table1Props = new HashMap<>();
        table1Props.put("table.iterator.minc.combiner", "10," + TestCombiner.class.getName());
        table1Props.put("table.iterator.minc.combiner.opt.dummy", "true");
        conf.set("table1" + TableConfigurationUtil.TABLE_CONFIGURATION_PROPERTY, encodeTableConfig(table1Props));
        conf.set("table2" + TableConfigurationUtil.TABLE_CONFIGURATION_PROPERTY, encodeTableConfig(new HashMap<>()));

        context = Mockito.mock(TaskInputOutputContext.class);
        Mockito.when(context.getConfiguration()).thenReturn(conf);
        Mockito.when(context.getCounter(Mockito.anyString(), Mockito.anyString())).thenAnswer(inv -> new GenericCounter());
        Mockito.when(context.getCounter(Mockito.any(Enum.class))).thenAnswer(inv -> new GenericCounter());

        writer = new TableCachingContextWriter();
    }

    private static String encodeTableConfig(Map<String,String> props) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new HashMap<>(props));
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    @Test
    public void testTimestampDedupRegression_emitsUnderRewrittenKey() throws Exception {
        writer.setup(conf, false);

        Key k1 = new Key(new Text("row1"), TIMESTAMP);
        Key k2 = new Key(new Text("row1"), TIMESTAMP);
        BulkIngestKey bik1 = new BulkIngestKey(new Text("table1"), k1);
        BulkIngestKey bik2 = new BulkIngestKey(new Text("table1"), k2);
        Value v1 = new Value("first".getBytes());
        Value v2 = new Value("second".getBytes());

        writer.write(bik1, v1, context);
        writer.write(bik2, v2, context);
        writer.cleanup(context);

        assertEquals(1, CaptureContextWriter.captured.size());
        Map.Entry<BulkIngestKey,Value> entry = CaptureContextWriter.captured.entries().iterator().next();

        long expectedTs = -1 * (TIMESTAMP / AggregatingReducer.MILLISPERDAY);
        assertEquals(new Text("table1"), entry.getKey().getTableName());
        assertEquals(expectedTs, entry.getKey().getKey().getTimestamp());
        assertEquals(v1, entry.getValue());

        for (BulkIngestKey k : CaptureContextWriter.captured.keySet()) {
            assertNotEquals(TIMESTAMP, k.getKey().getTimestamp());
        }
    }

    @Test
    public void testSingleValue_emitsUnderOriginalKey() throws Exception {
        writer.setup(conf, false);

        Key k = new Key(new Text("row2"), TIMESTAMP);
        BulkIngestKey bik = new BulkIngestKey(new Text("table1"), k);
        Value v = new Value("only".getBytes());

        writer.write(bik, v, context);
        writer.cleanup(context);

        assertEquals(1, CaptureContextWriter.captured.size());
        Map.Entry<BulkIngestKey,Value> entry = CaptureContextWriter.captured.entries().iterator().next();
        assertEquals(bik, entry.getKey());
        assertEquals(TIMESTAMP, entry.getKey().getKey().getTimestamp());
        assertEquals(v, entry.getValue());
    }

    @Test
    public void testNonCachedTable_passesThroughOnCommit() throws Exception {
        writer.setup(conf, false);

        Key k = new Key(new Text("row3"), 42L);
        BulkIngestKey bik = new BulkIngestKey(new Text("table2"), k);
        Value v = new Value("passthrough".getBytes());

        writer.write(bik, v, context);
        writer.commit(context);

        assertEquals(1, CaptureContextWriter.captured.size());
        Map.Entry<BulkIngestKey,Value> entry = CaptureContextWriter.captured.entries().iterator().next();
        assertEquals(bik, entry.getKey());
        assertEquals(v, entry.getValue());
    }

    /**
     * A combiner iterator class solely to make TableConfigurationUtil classify table1 as combiner-configured; reduce() is never invoked by the TS-dedup path.
     */
    public static class TestCombiner extends Combiner {
        public TestCombiner() {}

        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
            // no-op, mirrors BulkIngestKeyAggregatingReducerTest's testCombiner
        }

        @Override
        public Value reduce(Key key, Iterator<Value> iter) {
            Value last = null;
            while (iter.hasNext()) {
                last = iter.next();
            }
            return last;
        }
    }

    /**
     * Captures everything written to it, standing in for the chained context writer so we can inspect exactly what keys/values TableCachingContextWriter emits.
     */
    public static class CaptureContextWriter implements ContextWriter<BulkIngestKey,Value> {
        static Multimap<BulkIngestKey,Value> captured = ArrayListMultimap.create();

        public CaptureContextWriter() {}

        @Override
        public void setup(Configuration conf, boolean outputTableCounters) {}

        @Override
        public void write(BulkIngestKey key, Value value, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {
            captured.put(key, value);
        }

        @Override
        public void write(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {
            captured.putAll(entries);
        }

        @Override
        public void commit(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {}

        @Override
        public void rollback() {}

        @Override
        public void cleanup(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {}
    }
}
