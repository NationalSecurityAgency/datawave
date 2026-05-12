package datawave.ingest.mapreduce.job.reduce;

import static datawave.ingest.mapreduce.job.TableConfigurationUtil.ITERATOR_CLASS_MARKER;
import static datawave.ingest.mapreduce.job.TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES;
import static datawave.ingest.mapreduce.job.reduce.AggregatingReducer.MILLISPERDAY;
import static datawave.ingest.mapreduce.job.reduce.AggregatingReducer.USE_AGGREGATOR_PROPERTY;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_CLASS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.VERBOSE_COUNTERS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.VERBOSE_PARTITIONING_COUNTERS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metric.IngestOutput;

public class BulkIngestKeyAggregatingReducerTest {

    private Set<String> tables = ImmutableSet.of("table1", "table2", "table3", "table4");
    private Multimap<BulkIngestKey,Value> output;
    private Multimap<BulkIngestKey,Value> expected;
    private BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer;
    private Configuration conf;
    private ContextWriter<Key,Value> contextWriter;
    private TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context;
    private TableConfigurationUtil tcu;
    private Random rand = new Random();

    private MockedConstruction<TableConfigurationUtil> tcuConstruction;

    private Counter duplicateKey;
    private Counter r1Counter;
    private Counter r2Counter;
    private Counter r3Counter;
    private Counter tab1Counter;
    private Counter tab2Counter;
    private Counter tab3Counter;
    private Counter combinerCounter;
    private Counter negativeTimestampCounter;
    private Counter dupCounter;

    private int expectedDuplicateKey;
    private int expectedR1Counter;
    private int expectedR2Counter;
    private int expectedR3Counter;
    private int expectedTab1Counter;
    private int expectedTab2Counter;
    private int expectedTab3Counter;
    private int expectedCombinerCounter;
    private int expectedNegativeTimestampCounter;
    private int expectedDupCounter;

    private TaskID taskID;
    private TaskAttemptID taskAttemptID;

    private enum ExpectedValueType {
        NO_VALUE, FIRST_VALUE, COMBINED_VALUES, ALL_VALUES
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        expected = ArrayListMultimap.create();
        output = ArrayListMultimap.create();
        duplicateKey = (Counter) new GenericCounter();
        duplicateKey = (Counter) new GenericCounter();
        r1Counter = (Counter) new GenericCounter();
        r2Counter = (Counter) new GenericCounter();
        r3Counter = (Counter) new GenericCounter();
        tab1Counter = (Counter) new GenericCounter();
        tab2Counter = (Counter) new GenericCounter();
        tab3Counter = (Counter) new GenericCounter();
        combinerCounter = (Counter) new GenericCounter();
        negativeTimestampCounter = (Counter) new GenericCounter();
        dupCounter = (Counter) new GenericCounter();

        expectedDuplicateKey = 0;
        expectedR1Counter = 0;
        expectedR2Counter = 0;
        expectedR3Counter = 0;
        expectedTab1Counter = 0;
        expectedTab2Counter = 0;
        expectedTab3Counter = 0;
        expectedCombinerCounter = 0;
        expectedNegativeTimestampCounter = 0;
        expectedDupCounter = 0;

        // Use a real Configuration and set properties as needed
        conf = new Configuration(false);
        conf.setStrings(JOB_OUTPUT_TABLE_NAMES, tables.toArray(new String[0]));
        conf.setBoolean(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, false);
        conf.setClass(CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);

        // Default all tables to NOT use aggregators (original mock behavior returned false for unstubbed calls)
        for (String table : tables) {
            conf.setBoolean(table + USE_AGGREGATOR_PROPERTY, false);
        }

        // Create tcu mock eagerly so helpers can configure stubs before reducer.setup()
        tcu = Mockito.mock(TableConfigurationUtil.class);

        // Mock TableConfigurationUtil construction so new TableConfigurationUtil(conf) delegates to our tcu mock
        tcuConstruction = Mockito.mockConstruction(TableConfigurationUtil.class, (mock, ctx) -> {
            Mockito.when(mock.getTableCombiners(Mockito.anyString())).thenAnswer(inv -> tcu.getTableCombiners(inv.getArgument(0)));
            Mockito.when(mock.getTableAggregators(Mockito.anyString())).thenAnswer(inv -> tcu.getTableAggregators(inv.getArgument(0)));
        });

        context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) Mockito.mock(TaskInputOutputContext.class);
        Mockito.doAnswer(invocation -> {
            BulkIngestKey k = invocation.getArgument(0);
            Value v = invocation.getArgument(1);
            output.put(k, v);
            return null;
        }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
        Mockito.when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
        Mockito.when(context.getConfiguration()).thenReturn(conf);

        reducer = new BulkIngestKeyAggregatingReducer<>();
    }

    @After
    public void tearDown() {
        if (tcuConstruction != null) {
            tcuConstruction.close();
        }
    }

    private void setupVerboseCounters() {
        tab1Counter = (Counter) new GenericCounter();
        tab2Counter = (Counter) new GenericCounter();
        tab3Counter = (Counter) new GenericCounter();

        conf.setBoolean(VERBOSE_COUNTERS, true);
        Mockito.when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table1 reducer"))).thenReturn(tab1Counter);
        Mockito.when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table2 reducer"))).thenReturn(tab2Counter);
        Mockito.when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table3 reducer"))).thenReturn(tab3Counter);
    }

    private void setupVerbosePartitioningCounters() {
        r1Counter = (Counter) new GenericCounter();
        r2Counter = (Counter) new GenericCounter();
        r3Counter = (Counter) new GenericCounter();
        tab1Counter = (Counter) new GenericCounter();
        tab2Counter = (Counter) new GenericCounter();
        tab3Counter = (Counter) new GenericCounter();

        conf.setBoolean(VERBOSE_PARTITIONING_COUNTERS, true);

        Mockito.when(context.getCounter(Mockito.eq("REDUCER 1"), Mockito.startsWith("TABLE table"))).thenReturn(r1Counter);
        Mockito.when(context.getCounter(Mockito.eq("REDUCER 2"), Mockito.startsWith("TABLE table"))).thenReturn(r2Counter);
        Mockito.when(context.getCounter(Mockito.eq("REDUCER 3"), Mockito.startsWith("TABLE table"))).thenReturn(r3Counter);

        Mockito.when(context.getCounter(Mockito.eq("TABLE table1"), Mockito.startsWith("REDUCER"))).thenReturn(tab1Counter);
        Mockito.when(context.getCounter(Mockito.eq("TABLE table2"), Mockito.startsWith("REDUCER"))).thenReturn(tab2Counter);
        Mockito.when(context.getCounter(Mockito.eq("TABLE table3"), Mockito.startsWith("REDUCER"))).thenReturn(tab3Counter);

        taskAttemptID = Mockito.mock(TaskAttemptID.class);
        taskID = Mockito.mock(TaskID.class);
        Mockito.when(taskAttemptID.getTaskID()).thenReturn(taskID);
        Mockito.when(taskAttemptID.getTaskType()).thenReturn(TaskType.REDUCE);

        Mockito.when(context.getTaskAttemptID()).thenReturn(taskAttemptID);

    }

    private void setupTimestampDedup() {
        conf.setBoolean("table1" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table2" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table3" + USE_AGGREGATOR_PROPERTY, true);
        Mockito.when(context.getCounter(IngestOutput.TIMESTAMP_DUPLICATE)).thenReturn(dupCounter);

        reducer.TSDedupTables.addAll(Arrays.asList(new Text("table1"), new Text("table2"), new Text("table3")));

        Map<String,String> optMap;
        optMap = new HashMap<>();
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");

        Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
        table1CombinerMap.put(1, optMap);
        Mockito.when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);

        Map<Integer,Map<String,String>> table2CombinerMap = new HashMap<>();
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
        table2CombinerMap.put(1, optMap);

        Mockito.when(tcu.getTableCombiners(Mockito.eq("table2"))).thenReturn(table2CombinerMap);
    }

    private void setupUsingCombiner() {
        combinerCounter = new GenericCounter();

        conf.setBoolean("table1" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table2" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table3" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean(BulkIngestKeyDedupeCombiner.USING_COMBINER, true);
        Mockito.when(context.getCounter(IngestOutput.MERGED_VALUE)).thenReturn(combinerCounter);

        Map<String,String> optMap;
        optMap = new HashMap<>();
        optMap.put("combiner", "");
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");

        Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
        table1CombinerMap.put(1, optMap);
        Mockito.when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);

        Map<Integer,Map<String,String>> table3CombinerMap = new HashMap<>();
        table3CombinerMap.put(1, optMap);

        Mockito.when(tcu.getTableCombiners(Mockito.eq("table3"))).thenReturn(table3CombinerMap);
    }

    private void checkCounterValues() {
        assertEquals(expectedDuplicateKey, duplicateKey.getValue());
        assertEquals(expectedR1Counter, r1Counter.getValue());
        assertEquals(expectedR2Counter, r2Counter.getValue());
        assertEquals(expectedR3Counter, r3Counter.getValue());
        assertEquals(expectedTab1Counter, tab1Counter.getValue());
        assertEquals(expectedTab2Counter, tab2Counter.getValue());
        assertEquals(expectedTab3Counter, tab3Counter.getValue());
        assertEquals(expectedCombinerCounter, combinerCounter.getValue());
        assertEquals(expectedNegativeTimestampCounter, negativeTimestampCounter.getValue());
    }

    @Test
    public void testDedupKeysOneTable() throws Exception {
        conf.setBoolean("table1" + USE_AGGREGATOR_PROPERTY, true);
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r3", 1, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r4", 0, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r1", 2, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r2", 5, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, ExpectedValueType.ALL_VALUES);

        expectedDuplicateKey = 2;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testDedupKeysTwoTables() throws Exception {
        conf.setBoolean("table1" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table2" + USE_AGGREGATOR_PROPERTY, true);
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r3", 1, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r1", 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r2", 3, ExpectedValueType.ALL_VALUES);

        expectedDuplicateKey = 4;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testVerboseCounters() throws Exception {
        setupVerboseCounters();
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, ExpectedValueType.ALL_VALUES);
        performDoReduce("table1", "r2", 3, ExpectedValueType.ALL_VALUES);
        performDoReduce("table1", "r3", 1, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r1", 2, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, ExpectedValueType.ALL_VALUES);
        performDoReduce("table3", "r2", 3, ExpectedValueType.ALL_VALUES);

        expectedTab1Counter = 8;
        expectedTab2Counter = 5;
        expectedTab3Counter = 3;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testDedupKeysWithVerboseCounters() throws Exception {
        setupVerboseCounters();
        conf.setBoolean("table1" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table2" + USE_AGGREGATOR_PROPERTY, true);
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r3", 1, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r1", 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r2", 3, ExpectedValueType.ALL_VALUES);

        expectedDuplicateKey = 4;
        expectedTab1Counter = 8;
        expectedTab2Counter = 5;
        expectedTab3Counter = 3;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testVerbosePartitioningCounters() throws Exception {
        setupVerbosePartitioningCounters();
        reducer.setup(conf);

        Mockito.when(taskID.getId()).thenReturn(1);
        performDoReduce("table1", "r1", 4, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(1);
        performDoReduce("table1", "r2", 3, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(2);
        performDoReduce("table1", "r3", 1, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(2);
        performDoReduce("table2", "r1", 2, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(2);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(3);
        performDoReduce("table2", "r3", 3, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(3);
        performDoReduce("table3", "r2", 3, ExpectedValueType.ALL_VALUES);

        expectedR1Counter = 2;
        expectedR2Counter = 3;
        expectedR3Counter = 2;
        expectedTab1Counter = 3;
        expectedTab2Counter = 3;
        expectedTab3Counter = 1;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testDedupKeysWithVerbosePartitioningCounters() throws Exception {
        setupVerbosePartitioningCounters();
        conf.setBoolean("table1" + USE_AGGREGATOR_PROPERTY, true);
        conf.setBoolean("table2" + USE_AGGREGATOR_PROPERTY, true);

        reducer.setup(conf);

        Mockito.when(taskID.getId()).thenReturn(1);
        performDoReduce("table1", "r1", 4, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, ExpectedValueType.FIRST_VALUE);
        Mockito.when(taskID.getId()).thenReturn(2);
        performDoReduce("table1", "r3", 1, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r1", 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(3);
        performDoReduce("table2", "r3", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r2", 3, ExpectedValueType.ALL_VALUES);

        expectedDuplicateKey = 4;
        expectedR1Counter = 2;
        expectedR2Counter = 3;
        expectedR3Counter = 2;
        expectedTab1Counter = 3;
        expectedTab2Counter = 3;
        expectedTab3Counter = 1;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testTimestampDedup() throws Exception {
        setupTimestampDedup();
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r1", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 2, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r3", 1, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r1", 2, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.NO_VALUE);
        performDoReduce("table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.NO_VALUE);
        performDoReduce("table2", "r3", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r3", 3, 4 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r2", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table4", "r2", 4, 3 * MILLISPERDAY, ExpectedValueType.ALL_VALUES);

        expectedDuplicateKey = 1;
        expectedDupCounter = 13;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testTimestampDedupWithVerboseCounters() throws Exception {
        setupTimestampDedup();
        setupVerboseCounters();
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r1", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 2, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r3", 1, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r1", 2, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.NO_VALUE);
        performDoReduce("table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.NO_VALUE);
        performDoReduce("table2", "r3", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r3", 3, 4 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r2", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);

        expectedDuplicateKey = 1;
        expectedDupCounter = 13;
        expectedTab1Counter = 13;
        expectedTab2Counter = 8;
        expectedTab3Counter = 3;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testTimestampDedupWithVerbosePartitioningCounters() throws Exception {
        setupTimestampDedup();
        setupVerbosePartitioningCounters();
        reducer.setup(conf);

        Mockito.when(taskID.getId()).thenReturn(1);
        performDoReduce("table1", "r1", 4, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r1", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table1", "r2", 2, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        Mockito.when(taskID.getId()).thenReturn(2);
        performDoReduce("table1", "r3", 1, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r1", 2, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.NO_VALUE);
        performDoReduce("table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.NO_VALUE);
        Mockito.when(taskID.getId()).thenReturn(3);
        performDoReduce("table2", "r3", 3, 4 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r2", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE);

        expectedDupCounter = 13;
        expectedR1Counter = 4;
        expectedR2Counter = 4;
        expectedR3Counter = 2;
        expectedTab1Counter = 5;
        expectedTab2Counter = 4;
        expectedTab3Counter = 1;
        expectedDuplicateKey = 1;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testUsingCombiner() throws Exception {
        setupUsingCombiner();
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r2", 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r3", 1, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table2", "r1", 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r1", 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table3", "r2", 0, ExpectedValueType.COMBINED_VALUES);

        expectedDuplicateKey = 2;
        expectedCombinerCounter = 5;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testUsingCombinerWithVerboseCounters() throws Exception {
        setupUsingCombiner();
        setupVerboseCounters();
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r2", 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r3", 1, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table2", "r1", 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r1", 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table3", "r2", 0, ExpectedValueType.COMBINED_VALUES);

        expectedDuplicateKey = 2;
        expectedCombinerCounter = 5;
        expectedTab1Counter = 8;
        expectedTab2Counter = 5;
        expectedTab3Counter = 3;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testUsingCombinerWithVerbosePartitioningCounters() throws Exception {
        setupUsingCombiner();
        setupVerbosePartitioningCounters();
        reducer.setup(conf);

        Mockito.when(taskID.getId()).thenReturn(1);
        performDoReduce("table1", "r1", 4, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r2", 3, ExpectedValueType.COMBINED_VALUES);
        Mockito.when(taskID.getId()).thenReturn(2);
        performDoReduce("table1", "r3", 1, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table2", "r1", 2, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, ExpectedValueType.ALL_VALUES);
        Mockito.when(taskID.getId()).thenReturn(3);
        performDoReduce("table2", "r3", 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r1", 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table3", "r2", 0, ExpectedValueType.COMBINED_VALUES);

        expectedDuplicateKey = 2;
        expectedCombinerCounter = 5;
        expectedR1Counter = 2;
        expectedR2Counter = 3;
        expectedR3Counter = 3;
        expectedTab1Counter = 3;
        expectedTab2Counter = 3;
        expectedTab3Counter = 2;
        checkCounterValues();
        assertEquals(expected, output);
    }

    @Test
    public void testUsingCombinerWithNegativeTimestamps() throws Exception {
        setupUsingCombiner();
        reducer.setup(conf);

        performDoReduce("table1", "r1", 4, -3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r3", 1, -3 * MILLISPERDAY, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table2", "r1", 2, -2 * MILLISPERDAY + MILLISPERDAY, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table2", "r2", 0, -2 * MILLISPERDAY + MILLISPERDAY, ExpectedValueType.ALL_VALUES);
        performDoReduce("table2", "r3", 3, -2 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE);
        performDoReduce("table3", "r1", 3, -4 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table3", "r2", 0, -4 * MILLISPERDAY, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r1", 4, 4 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.COMBINED_VALUES);
        performDoReduce("table1", "r2", 3, 2 * MILLISPERDAY + MILLISPERDAY, ExpectedValueType.COMBINED_VALUES);

        expectedDuplicateKey = 2;
        expectedCombinerCounter = 7;
        expectedNegativeTimestampCounter = 7;
        checkCounterValues();
        assertEquals(expected, output);
    }

    private void performDoReduce(String table, String row, int numberOfValues) throws Exception {
        performDoReduce(table, row, numberOfValues, 1L, ExpectedValueType.FIRST_VALUE);
    }

    private void performDoReduce(String table, String row, int numberOfValues, long ts) throws Exception {
        performDoReduce(table, row, numberOfValues, ts, ExpectedValueType.FIRST_VALUE);
    }

    private void performDoReduce(String table, String row, int numberOfValues, ExpectedValueType expectedValueType) throws Exception {
        performDoReduce(table, row, numberOfValues, 1L, expectedValueType);
    }

    private void performDoReduce(String table, String row, int numberOfValues, long ts, ExpectedValueType expectedValueType) throws Exception {
        Key key = new Key(new Text(row), ts);
        BulkIngestKey bulkIngestKey = new BulkIngestKey(new Text(table), key);
        List<Value> values = new ArrayList<>();
        Value value = new Value(new Text(String.format("%015d", rand.nextInt())));
        if (expectedValueType == ExpectedValueType.FIRST_VALUE) {
            expected.put(bulkIngestKey, value);
        }
        for (int i = 0; i < numberOfValues; i++) {
            values.add(value);
            value = new Value(new Text(String.format("%015d", rand.nextInt())));
        }

        if (expectedValueType == ExpectedValueType.COMBINED_VALUES) {
            expected.put(bulkIngestKey, combineValues(values.iterator()));
        } else if (expectedValueType == ExpectedValueType.ALL_VALUES) {
            expected.putAll(bulkIngestKey, values);
        }

        reducer.doReduce(bulkIngestKey, values, context);

        if (bulkIngestKey.getKey().getTimestamp() < 0) {
            negativeTimestampCounter.increment(1);
        }
    }

    public static Value combineValues(Iterator<Value> iter) {
        StringBuilder combinedValues = new StringBuilder();
        iter.forEachRemaining(value -> combinedValues.append(value.toString()));
        Value value = new Value(new Text(combinedValues.toString()));
        return value;
    }

    public static class testCombiner extends Combiner {
        public testCombiner() {}

        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {

        }

        @Override
        public Value reduce(Key key, Iterator<Value> iter) {
            return combineValues(iter);
        }

    }

}
