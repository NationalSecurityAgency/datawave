package datawave.ingest.mapreduce.job.reduce;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.metric.IngestOutput;
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
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static datawave.ingest.mapreduce.job.TableConfigurationUtil.ITERATOR_CLASS_MARKER;
import static datawave.ingest.mapreduce.job.reduce.AggregatingReducer.MILLISPERDAY;
import static datawave.ingest.mapreduce.job.reduce.AggregatingReducer.USE_AGGREGATOR_PROPERTY;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_CLASS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.VERBOSE_COUNTERS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducer.VERBOSE_PARTITIONING_COUNTERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkIngestKeyAggregatingReducerTest {
    
    static Set<String> tables = ImmutableSet.of("table1", "table2", "table3", "table4");
    static Random rand = new Random();
    
    private enum ExpectedValueType {
        NO_VALUE, FIRST_VALUE, COMBINED_VALUES, ALL_VALUES
    }
    
    @Test
    public void testDedupKeysOneTable() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames(Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r4", 0, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r2", 5, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedDuplicateKey = 2;
                    assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
        
    }
    
    @Test
    public void testDedupKeysTwoTables() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames(Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedDuplicateKey = 4;
                    assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testVerboseCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table1 reducer"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table2 reducer"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table3 reducer"))).thenReturn(tab3Counter);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedTab1Counter = 8;
                    expectedTab2Counter = 5;
                    expectedTab3Counter = 3;
                    assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testDedupKeysWithVerboseCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table1 reducer"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table2 reducer"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table3 reducer"))).thenReturn(tab3Counter);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedDuplicateKey = 4;
                    expectedTab1Counter = 8;
                    expectedTab2Counter = 5;
                    expectedTab3Counter = 3;
                    assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testVerbosePartitioningCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_PARTITIONING_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    
                    when(context.getCounter(Mockito.eq("REDUCER 1"), Mockito.startsWith("TABLE table"))).thenReturn(r1Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 2"), Mockito.startsWith("TABLE table"))).thenReturn(r2Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 3"), Mockito.startsWith("TABLE table"))).thenReturn(r3Counter);
                    
                    when(context.getCounter(Mockito.eq("TABLE table1"), Mockito.startsWith("REDUCER"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE table2"), Mockito.startsWith("REDUCER"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE table3"), Mockito.startsWith("REDUCER"))).thenReturn(tab3Counter);
                    
                    TaskAttemptID taskAttemptID = mock(TaskAttemptID.class);
                    TaskID taskID = mock(TaskID.class);
                    when(taskAttemptID.getTaskID()).thenReturn(taskID);
                    when(taskAttemptID.getTaskType()).thenReturn(TaskType.REDUCE);
                    
                    when(context.getTaskAttemptID()).thenReturn(taskAttemptID);
                    
                    reducer.setup(conf);
                    
                    when(taskID.getId()).thenReturn(1);
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(1);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(2);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(2);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(2);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(3);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(3);
                    performDoReduce(reducer, expected, "table3", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedR1Counter = 2;
                    expectedR2Counter = 3;
                    expectedR3Counter = 2;
                    expectedTab1Counter = 3;
                    expectedTab2Counter = 3;
                    expectedTab3Counter = 1;
                    assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testDedupKeysWithVerbosePartitioningCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_PARTITIONING_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    
                    when(context.getCounter(Mockito.eq("REDUCER 1"), Mockito.startsWith("TABLE table"))).thenReturn(r1Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 2"), Mockito.startsWith("TABLE table"))).thenReturn(r2Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 3"), Mockito.startsWith("TABLE table"))).thenReturn(r3Counter);
                    
                    when(context.getCounter(Mockito.eq("TABLE table1"), Mockito.startsWith("REDUCER"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE table2"), Mockito.startsWith("REDUCER"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE table3"), Mockito.startsWith("REDUCER"))).thenReturn(tab3Counter);
                    
                    TaskAttemptID taskAttemptID = mock(TaskAttemptID.class);
                    TaskID taskID = mock(TaskID.class);
                    when(taskAttemptID.getTaskID()).thenReturn(taskID);
                    when(taskAttemptID.getTaskType()).thenReturn(TaskType.REDUCE);
                    
                    when(context.getTaskAttemptID()).thenReturn(taskAttemptID);
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    
                    reducer.setup(conf);
                    
                    when(taskID.getId()).thenReturn(1);
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.FIRST_VALUE, context);
                    when(taskID.getId()).thenReturn(2);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(3);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedDuplicateKey = 4;
                    expectedR1Counter = 2;
                    expectedR2Counter = 3;
                    expectedR3Counter = 2;
                    expectedTab1Counter = 3;
                    expectedTab2Counter = 3;
                    expectedTab3Counter = 1;
                    assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testTimestampDedup() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    int expectedDupCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames(Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table3" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(context.getCounter(IngestOutput.TIMESTAMP_DUPLICATE)).thenReturn(dupCounter);
                    
                    reducer.TSDedupTables.addAll(Arrays.asList(new Text("table1"), new Text("table2"), new Text("table3")));
                    
                    Map<String,String> optMap;
                    optMap = new HashMap<>();
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    
                    Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
                    table1CombinerMap.put(1, optMap);
                    when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);
                    
                    Map<Integer,Map<String,String>> table2CombinerMap = new HashMap<>();
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    table2CombinerMap.put(1, optMap);
                    
                    when(tcu.getTableCombiners(Mockito.eq("table2"))).thenReturn(table2CombinerMap);
                    
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r1", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 2, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.NO_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.NO_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, 4 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table4", "r2", 4, 3 * MILLISPERDAY, ExpectedValueType.ALL_VALUES, context);
                    
                    expectedDuplicateKey = 1;
                    expectedDupCounter = 13;
                    // assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testTimestampDedupWithVerboseCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    int expectedDupCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) Mockito
                                    .mock(TaskInputOutputContext.class);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table1 reducer"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table2 reducer"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table3 reducer"))).thenReturn(tab3Counter);
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table3" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(context.getCounter(IngestOutput.TIMESTAMP_DUPLICATE)).thenReturn(dupCounter);
                    
                    reducer.TSDedupTables.addAll(Arrays.asList(new Text("table1"), new Text("table2"), new Text("table3")));
                    
                    Map<String,String> optMap;
                    optMap = new HashMap<>();
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    
                    Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
                    table1CombinerMap.put(1, optMap);
                    when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);
                    
                    Map<Integer,Map<String,String>> table2CombinerMap = new HashMap<>();
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    table2CombinerMap.put(1, optMap);
                    
                    when(tcu.getTableCombiners(Mockito.eq("table2"))).thenReturn(table2CombinerMap);
                    
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r1", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 2, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.NO_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.NO_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, 4 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    
                    expectedDuplicateKey = 1;
                    expectedDupCounter = 13;
                    expectedTab1Counter = 13;
                    expectedTab2Counter = 8;
                    expectedTab3Counter = 3;
                    // assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testTimestampDedupWithVerbosePartitioningCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    int expectedDupCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames(Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table3" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(context.getCounter(IngestOutput.TIMESTAMP_DUPLICATE)).thenReturn(dupCounter);
                    
                    reducer.TSDedupTables.addAll(Arrays.asList(new Text("table1"), new Text("table2"), new Text("table3")));
                    
                    Map<String,String> optMap;
                    optMap = new HashMap<>();
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    
                    Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
                    table1CombinerMap.put(1, optMap);
                    when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);
                    
                    Map<Integer,Map<String,String>> table2CombinerMap = new HashMap<>();
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    table2CombinerMap.put(1, optMap);
                    
                    when(tcu.getTableCombiners(Mockito.eq("table2"))).thenReturn(table2CombinerMap);
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_PARTITIONING_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    
                    when(context.getCounter(Mockito.eq("REDUCER 1"), Mockito.startsWith("TABLE table"))).thenReturn(r1Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 2"), Mockito.startsWith("TABLE table"))).thenReturn(r2Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 3"), Mockito.startsWith("TABLE table"))).thenReturn(r3Counter);
                    
                    when(context.getCounter(Mockito.eq("TABLE table1"), Mockito.startsWith("REDUCER"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE table2"), Mockito.startsWith("REDUCER"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE table3"), Mockito.startsWith("REDUCER"))).thenReturn(tab3Counter);
                    
                    TaskAttemptID taskAttemptID = mock(TaskAttemptID.class);
                    TaskID taskID = mock(TaskID.class);
                    when(taskAttemptID.getTaskID()).thenReturn(taskID);
                    when(taskAttemptID.getTaskType()).thenReturn(TaskType.REDUCE);
                    
                    when(context.getTaskAttemptID()).thenReturn(taskAttemptID);
                    
                    reducer.setup(conf);
                    
                    when(taskID.getId()).thenReturn(1);
                    performDoReduce(reducer, expected, "table1", "r1", 4, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r1", 3, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table1", "r2", 2, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.FIRST_VALUE, context);
                    when(taskID.getId()).thenReturn(2);
                    performDoReduce(reducer, expected, "table1", "r3", 1, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 2, ExpectedValueType.NO_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, 3 * MILLISPERDAY + MILLISPERDAY / 3, ExpectedValueType.NO_VALUE, context);
                    when(taskID.getId()).thenReturn(3);
                    performDoReduce(reducer, expected, "table2", "r3", 3, 4 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r2", 3, 3 * MILLISPERDAY, ExpectedValueType.FIRST_VALUE, context);
                    
                    expectedDupCounter = 13;
                    expectedR1Counter = 4;
                    expectedR2Counter = 4;
                    expectedR3Counter = 2;
                    expectedTab1Counter = 5;
                    expectedTab2Counter = 4;
                    expectedTab3Counter = 1;
                    expectedDuplicateKey = 1;
                    // assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    // assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    assertEquals(expected, output);
                    
                }
            }
        }
    }
    
    @Test
    public void testUsingCombiner() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    int expectedDupCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) mock(TaskInputOutputContext.class);
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table3" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq(BulkIngestKeyDedupeCombiner.USING_COMBINER), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(IngestOutput.MERGED_VALUE)).thenReturn(combinerCounter);
                    
                    Map<String,String> optMap;
                    optMap = new HashMap<>();
                    optMap.put("combiner", "");
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    
                    Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
                    table1CombinerMap.put(1, optMap);
                    when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);
                    
                    Map<Integer,Map<String,String>> table3CombinerMap = new HashMap<>();
                    table3CombinerMap.put(1, optMap);
                    
                    when(tcu.getTableCombiners(Mockito.eq("table3"))).thenReturn(table3CombinerMap);
                    
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r1", 3, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table3", "r2", 0, ExpectedValueType.COMBINED_VALUES, context);
                    
                    expectedDuplicateKey = 2;
                    expectedCombinerCounter = 5;
                    // assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    // assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    // assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testUsingCombinerWithVerboseCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    int expectedDupCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) Mockito
                                    .mock(TaskInputOutputContext.class);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table1 reducer"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table2 reducer"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE.KEY.VALUElen"), Mockito.startsWith("table3 reducer"))).thenReturn(tab3Counter);
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table3" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq(BulkIngestKeyDedupeCombiner.USING_COMBINER), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(IngestOutput.MERGED_VALUE)).thenReturn(combinerCounter);
                    
                    Map<String,String> optMap;
                    optMap = new HashMap<>();
                    optMap.put("combiner", "");
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    
                    Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
                    table1CombinerMap.put(1, optMap);
                    when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);
                    
                    Map<Integer,Map<String,String>> table3CombinerMap = new HashMap<>();
                    table3CombinerMap.put(1, optMap);
                    
                    when(tcu.getTableCombiners(Mockito.eq("table3"))).thenReturn(table3CombinerMap);
                    
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    reducer.setup(conf);
                    
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r1", 3, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table3", "r2", 0, ExpectedValueType.COMBINED_VALUES, context);
                    
                    expectedDuplicateKey = 2;
                    expectedCombinerCounter = 5;
                    expectedTab1Counter = 8;
                    expectedTab2Counter = 5;
                    expectedTab3Counter = 3;
                    // assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    // assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    // assertEquals(expected, output);
                }
            }
        }
    }
    
    @Test
    public void testUsingCombinerWithVerbosePartitioningCounters() throws Exception {
        try (MockedConstruction<Configuration> mockedConf = Mockito.mockConstruction(Configuration.class)) {
            
            try (MockedConstruction<TableConfigurationUtil> mockedTcu = Mockito.mockConstruction(TableConfigurationUtil.class)) {
                
                try (MockedStatic<TableConfigurationUtil> staticTcu = Mockito.mockStatic(TableConfigurationUtil.class)) {
                    BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer = new BulkIngestKeyAggregatingReducer<>();
                    
                    Map<String,String> confMap = new HashMap();
                    Multimap<BulkIngestKey,Value> expected = ArrayListMultimap.create();
                    Multimap<BulkIngestKey,Value> output = ArrayListMultimap.create();
                    Counter duplicateKey = new GenericCounter();
                    Counter r1Counter = new GenericCounter();
                    Counter r2Counter = new GenericCounter();
                    Counter r3Counter = new GenericCounter();
                    Counter tab1Counter = new GenericCounter();
                    Counter tab2Counter = new GenericCounter();
                    Counter tab3Counter = new GenericCounter();
                    Counter combinerCounter = new GenericCounter();
                    Counter dupCounter = new GenericCounter();
                    
                    int expectedDuplicateKey = 0;
                    int expectedR1Counter = 0;
                    int expectedR2Counter = 0;
                    int expectedR3Counter = 0;
                    int expectedTab1Counter = 0;
                    int expectedTab2Counter = 0;
                    int expectedTab3Counter = 0;
                    int expectedCombinerCounter = 0;
                    int expectedDupCounter = 0;
                    
                    staticTcu.when(() -> TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context = (TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value>) Mockito
                                    .mock(TaskInputOutputContext.class);
                    
                    Configuration conf = new Configuration();
                    when(conf.iterator()).thenReturn(confMap.entrySet().iterator());
                    when(conf.getBoolean(Mockito.eq(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS), Mockito.eq(false))).thenReturn(false);
                    Mockito.doReturn(BulkContextWriter.class).when(conf).getClass(Mockito.eq(CONTEXT_WRITER_CLASS), Mockito.any(), Mockito.any());
                    TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
                    Mockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
                    
                    combinerCounter = new GenericCounter();
                    
                    when(conf.getBoolean(Mockito.eq("table1" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table2" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq("table3" + USE_AGGREGATOR_PROPERTY), Mockito.eq(true))).thenReturn(true);
                    when(conf.getBoolean(Mockito.eq(BulkIngestKeyDedupeCombiner.USING_COMBINER), Mockito.eq(false))).thenReturn(true);
                    when(context.getCounter(IngestOutput.MERGED_VALUE)).thenReturn(combinerCounter);
                    
                    Map<String,String> optMap;
                    optMap = new HashMap<>();
                    optMap.put("combiner", "");
                    optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.BulkIngestKeyAggregatingReducerTest$testCombiner");
                    
                    Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
                    table1CombinerMap.put(1, optMap);
                    when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);
                    
                    Map<Integer,Map<String,String>> table3CombinerMap = new HashMap<>();
                    table3CombinerMap.put(1, optMap);
                    
                    when(tcu.getTableCombiners(Mockito.eq("table3"))).thenReturn(table3CombinerMap);
                    
                    when(conf.getBoolean(Mockito.eq(VERBOSE_PARTITIONING_COUNTERS), Mockito.eq(false))).thenReturn(true);
                    
                    when(context.getCounter(Mockito.eq("REDUCER 1"), Mockito.startsWith("TABLE table"))).thenReturn(r1Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 2"), Mockito.startsWith("TABLE table"))).thenReturn(r2Counter);
                    when(context.getCounter(Mockito.eq("REDUCER 3"), Mockito.startsWith("TABLE table"))).thenReturn(r3Counter);
                    
                    when(context.getCounter(Mockito.eq("TABLE table1"), Mockito.startsWith("REDUCER"))).thenReturn(tab1Counter);
                    when(context.getCounter(Mockito.eq("TABLE table2"), Mockito.startsWith("REDUCER"))).thenReturn(tab2Counter);
                    when(context.getCounter(Mockito.eq("TABLE table3"), Mockito.startsWith("REDUCER"))).thenReturn(tab3Counter);
                    
                    TaskAttemptID taskAttemptID = mock(TaskAttemptID.class);
                    TaskID taskID = mock(TaskID.class);
                    when(taskAttemptID.getTaskID()).thenReturn(taskID);
                    when(taskAttemptID.getTaskType()).thenReturn(TaskType.REDUCE);
                    
                    when(context.getTaskAttemptID()).thenReturn(taskAttemptID);
                    
                    if (null != context.getCounter(IngestOutput.DUPLICATE_KEY)) {
                        context.getCounter(IngestOutput.DUPLICATE_KEY).setValue(0L);
                    }
                    Mockito.doAnswer(invocation -> {
                        BulkIngestKey k = invocation.getArgument(0);
                        Value v = invocation.getArgument(1);
                        output.put(k, v);
                        return null;
                    }).when(context).write(Mockito.any(BulkIngestKey.class), Mockito.any(Value.class));
                    when(context.getCounter(IngestOutput.DUPLICATE_KEY)).thenReturn(duplicateKey);
                    
                    reducer.setup(conf);
                    
                    when(taskID.getId()).thenReturn(1);
                    performDoReduce(reducer, expected, "table1", "r1", 4, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table1", "r2", 3, ExpectedValueType.COMBINED_VALUES, context);
                    when(taskID.getId()).thenReturn(2);
                    performDoReduce(reducer, expected, "table1", "r3", 1, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table2", "r1", 2, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table2", "r2", 0, ExpectedValueType.ALL_VALUES, context);
                    when(taskID.getId()).thenReturn(3);
                    performDoReduce(reducer, expected, "table2", "r3", 3, ExpectedValueType.FIRST_VALUE, context);
                    performDoReduce(reducer, expected, "table3", "r1", 3, ExpectedValueType.COMBINED_VALUES, context);
                    performDoReduce(reducer, expected, "table3", "r2", 0, ExpectedValueType.COMBINED_VALUES, context);
                    
                    expectedDuplicateKey = 2;
                    expectedCombinerCounter = 5;
                    expectedR1Counter = 2;
                    expectedR2Counter = 3;
                    expectedR3Counter = 3;
                    expectedTab1Counter = 3;
                    expectedTab2Counter = 3;
                    expectedTab3Counter = 2;
                    // assertEquals(expectedDuplicateKey, duplicateKey.getValue());
                    assertEquals(expectedR1Counter, r1Counter.getValue());
                    assertEquals(expectedR2Counter, r2Counter.getValue());
                    assertEquals(expectedR3Counter, r3Counter.getValue());
                    assertEquals(expectedTab1Counter, tab1Counter.getValue());
                    assertEquals(expectedTab2Counter, tab2Counter.getValue());
                    assertEquals(expectedTab3Counter, tab3Counter.getValue());
                    // assertEquals(expectedCombinerCounter, combinerCounter.getValue());
                    // assertEquals(expected, output);
                }
            }
        }
    }
    
    private void performDoReduce(BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer, Multimap<BulkIngestKey,Value> expected, String table,
                    String row, int numberOfValues, TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context) throws Exception {
        performDoReduce(reducer, expected, table, row, numberOfValues, 1L, ExpectedValueType.FIRST_VALUE, context);
    }
    
    private void performDoReduce(BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer, Multimap<BulkIngestKey,Value> expected, String table,
                    String row, int numberOfValues, long ts, TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context) throws Exception {
        performDoReduce(reducer, expected, table, row, numberOfValues, ts, ExpectedValueType.FIRST_VALUE, context);
    }
    
    private void performDoReduce(BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer, Multimap<BulkIngestKey,Value> expected, String table,
                    String row, int numberOfValues, ExpectedValueType expectedValueType, TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context)
                    throws Exception {
        performDoReduce(reducer, expected, table, row, numberOfValues, 1L, expectedValueType, context);
    }
    
    private void performDoReduce(BulkIngestKeyAggregatingReducer<BulkIngestKey,Value> reducer, Multimap<BulkIngestKey,Value> expected, String table,
                    String row, int numberOfValues, long ts, ExpectedValueType expectedValueType,
                    TaskInputOutputContext<BulkIngestKey,Value,BulkIngestKey,Value> context) throws Exception {
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
