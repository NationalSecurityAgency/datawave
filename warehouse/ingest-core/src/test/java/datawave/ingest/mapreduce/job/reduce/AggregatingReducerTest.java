//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package datawave.ingest.mapreduce.job.reduce;

import static datawave.ingest.mapreduce.job.TableConfigurationUtil.ITERATOR_CLASS_MARKER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.ingest.mapreduce.job.reduce.AggregatingReducer.CustomColumnToClassMapping;

@RunWith(PowerMockRunner.class)
// @PrepareForTest({TableConfigurationUtil.class})
@PrepareForTest({AggregatingReducer.class, TableConfigurationUtil.class})
public class AggregatingReducerTest {
    private CustomColumnToClassMapping columnToClassMapping;
    private Map<String,String> optMap;
    private Map<String,Entry<Map<String,String>,String>> columnMap;
    private Set<String> tables = ImmutableSet.of("table1", "table2", "table3");
    private Configuration conf;
    TableConfigurationUtil tcu;
    Map<String,String> confMap;

    public AggregatingReducerTest() {

    }

    @Before
    public void setup() {
        columnMap = new HashMap();
        optMap = new HashMap();
        confMap = new HashMap();
    }

    private void setupOptMap() {
        optMap.put("fam1:qual1", AggregatingReducerTest.testCombiner1.class.getName());
        optMap.put("*", AggregatingReducerTest.testCombiner2.class.getName());
        optMap.put("fam3", AggregatingReducerTest.testCombiner3.class.getName());
    }

    private void setupColumnMap() {
        optMap.put("columns", "fam1:qual1,fam3");

        columnMap.put("fam1:qual1", Maps.immutableEntry(optMap, AggregatingReducerTest.testCombiner.class.getName()));
        columnMap.put("fam3", Maps.immutableEntry(optMap, AggregatingReducerTest.testCombiner1.class.getName()));
        columnMap.put("*", Maps.immutableEntry(optMap, AggregatingReducerTest.testCombiner2.class.getName()));
    }

    private void setupMocks() throws Exception {
        conf = (Configuration) PowerMockito.mock(Configuration.class);
        PowerMockito.whenNew(Configuration.class).withAnyArguments().thenReturn(conf);
        PowerMockito.when(conf.iterator()).thenReturn(confMap.entrySet().iterator());

        PowerMockito.mockStatic(TableConfigurationUtil.class, new Class[0]);
        PowerMockito.when(TableConfigurationUtil.getJobOutputTableNames((Configuration) Mockito.any(Configuration.class))).thenReturn(tables);

        tcu = PowerMockito.mock(TableConfigurationUtil.class);
        PowerMockito.whenNew(TableConfigurationUtil.class).withAnyArguments().thenReturn(tcu);
        PowerMockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
    }

    @Test
    public void testColumnToClassMappingOptMap() {
        setupOptMap();
        columnToClassMapping = new CustomColumnToClassMapping(1, optMap);

        Assert.assertEquals("testCombiner1", this.columnToClassMapping.getObject(new Key("", "fam1", "qual1")).toString());
        Assert.assertEquals("testCombiner2", this.columnToClassMapping.getObject(new Key("", "fam2", "qual2")).toString());
        Assert.assertEquals("testCombiner3", this.columnToClassMapping.getObject(new Key("", "fam3")).toString());
        Assert.assertEquals("testCombiner2", this.columnToClassMapping.getObject(new Key("", "fam2")).toString());
    }

    @Test
    public void testColumnToClassMappingOptMapPriorityComparison() {
        setupOptMap();

        CustomColumnToClassMapping columnToClassMapping1 = new CustomColumnToClassMapping(1, optMap);
        CustomColumnToClassMapping columnToClassMapping2 = new CustomColumnToClassMapping(2, optMap);
        CustomColumnToClassMapping columnToClassMapping3 = new CustomColumnToClassMapping(1, optMap);

        Assert.assertEquals(-1L, (long) columnToClassMapping1.compareTo(columnToClassMapping2));
        Assert.assertEquals(1L, (long) columnToClassMapping2.compareTo(columnToClassMapping1));
        Assert.assertEquals(0L, (long) columnToClassMapping1.compareTo(columnToClassMapping3));
    }

    @Test(expected = RuntimeException.class)
    public void testColumnToClassMappingOptMapInstantiationException() {
        optMap.put("fam4", "testCombiner4");
        this.columnToClassMapping = new CustomColumnToClassMapping(1, optMap);
        this.columnToClassMapping.getObject(new Key("", "fam2"));
    }

    @Test
    public void testColumnToClassMappingColMap() {
        setupOptMap();
        setupColumnMap();

        columnToClassMapping = new CustomColumnToClassMapping(columnMap, 1);

        Assert.assertEquals("testCombiner", this.columnToClassMapping.getObject(new Key("", "fam1", "qual1")).toString());
        Assert.assertEquals("testCombiner1", this.columnToClassMapping.getObject(new Key("", "fam3")).toString());
        Assert.assertEquals("testCombiner2", this.columnToClassMapping.getObject(new Key("", "fam2")).toString());
    }

    @Test
    public void testColumnToClassMappingColMapPriorityComparison() {
        setupColumnMap();

        CustomColumnToClassMapping columnToClassMapping1 = new CustomColumnToClassMapping(columnMap, 1);
        CustomColumnToClassMapping columnToClassMapping2 = new CustomColumnToClassMapping(columnMap, 2);
        CustomColumnToClassMapping columnToClassMapping3 = new CustomColumnToClassMapping(columnMap, 1);

        Assert.assertEquals(-1L, (long) columnToClassMapping1.compareTo(columnToClassMapping2));
        Assert.assertEquals(1L, (long) columnToClassMapping2.compareTo(columnToClassMapping1));
        Assert.assertEquals(0L, (long) columnToClassMapping1.compareTo(columnToClassMapping3));
    }

    @Test(expected = RuntimeException.class)
    public void testColumnToClassMappingColMapInstantiationException() {
        optMap.put("fam4", "testCombiner4");
        columnMap.put("fam3", Maps.immutableEntry(optMap, AggregatingReducerTest.testCombiner1.class.getName()));
        columnToClassMapping = new CustomColumnToClassMapping(columnMap, 1);
        columnToClassMapping.getObject(new Key("", "fam2"));
    }

    @Test
    public void testConfigureAggregators() throws Exception {
        Map<String,String> optMap;

        Map<Integer,Map<String,String>> table1AggregatorMap = new HashMap<>();
        optMap = Collections.singletonMap("fam1:qual1", "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner1");
        table1AggregatorMap.put(1, optMap);
        optMap = Collections.singletonMap("fam1:qual1", "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner2");
        table1AggregatorMap.put(2, optMap);

        Map<Integer,Map<String,String>> table2AggregatorMap = new HashMap<>();
        optMap = new HashMap<>();
        optMap.put("fam1:qual1", "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner3");
        optMap.put("fam2:qual2", "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner1");
        table2AggregatorMap.put(1, optMap);
        optMap = Collections.singletonMap("fam1:qual1", "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner");
        table2AggregatorMap.put(2, optMap);

        setupMocks();
        PowerMockito.when(tcu.getTableAggregators(Mockito.eq("table1"))).thenReturn(table1AggregatorMap);
        PowerMockito.when(tcu.getTableAggregators(Mockito.eq("table2"))).thenReturn(table2AggregatorMap);

        AggregatingReducerTest.TestAggregatingReducer aggregatingReducer = new AggregatingReducerTest.TestAggregatingReducer();
        aggregatingReducer.tcu = tcu;
        aggregatingReducer.configureAggregators(conf);

        assertCombineList(new String[] {"testCombiner1", "testCombiner2"}, aggregatingReducer, new Text("table1"), new Key("", "fam1", "qual1"));
        assertCombineList(new String[] {"testCombiner3", "testCombiner"}, aggregatingReducer, new Text("table2"), new Key("", "fam1", "qual1"));
        assertCombineList(new String[] {"testCombiner1"}, aggregatingReducer, new Text("table2"), new Key("", "fam2", "qual2"));
    }

    @Test
    public void testConfigureCombiners() throws Exception {
        Map<String,String> optMap;

        Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
        optMap = new HashMap<>();
        optMap.put("columns", "fam5,fam6");
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner3");
        table1CombinerMap.put(1, optMap);

        optMap = new HashMap<>();
        optMap.put("columns", "fam3,fam4");
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner");
        table1CombinerMap.put(2, optMap);
        optMap = Collections.singletonMap(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner2");
        table1CombinerMap.put(3, optMap);

        setupMocks();
        PowerMockito.when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);

        AggregatingReducerTest.TestAggregatingReducer aggregatingReducer = new AggregatingReducerTest.TestAggregatingReducer();
        aggregatingReducer.tcu = tcu;
        aggregatingReducer.configureCombiners(conf);

        this.assertCombineList(new String[] {"testCombiner2"}, aggregatingReducer, new Text("table1"), new Key("", "fam1", "qual1"));
        this.assertCombineList(new String[] {"testCombiner", "testCombiner2"}, aggregatingReducer, new Text("table1"), new Key("", "fam3", "qual2"));
        this.assertCombineList(new String[] {"testCombiner3", "testCombiner2"}, aggregatingReducer, new Text("table1"), new Key("", "fam5", "qual3"));
    }

    @Test(expected = RuntimeException.class)
    public void testConfigureCombinersMissingIterClazzSetting() throws Exception {
        Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
        optMap = new HashMap<>();
        optMap.put("columns", "fam5,fam6");
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner3");
        table1CombinerMap.put(1, optMap);
        optMap = Collections.singletonMap("columns", "fam3,fam4");
        table1CombinerMap.put(2, optMap);

        setupMocks();
        PowerMockito.whenNew(TableConfigurationUtil.class).withAnyArguments().thenReturn(tcu);
        PowerMockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
        PowerMockito.when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);

        AggregatingReducerTest.TestAggregatingReducer aggregatingReducer = new AggregatingReducerTest.TestAggregatingReducer();
        aggregatingReducer.configureCombiners(conf);
    }

    @Test(expected = RuntimeException.class)
    public void testConfigureCombinersInstantiationError() throws Exception {
        Map<Integer,Map<String,String>> table1CombinerMap = new HashMap<>();
        optMap = new HashMap<>();
        optMap.put("columns", "fam5,fam6");
        optMap.put(ITERATOR_CLASS_MARKER, "datawave.ingest.mapreduce.job.reduce.AggregatingReducerTest$testCombiner4");
        table1CombinerMap.put(1, optMap);

        setupMocks();
        TableConfigurationUtil tcu = PowerMockito.mock(TableConfigurationUtil.class);
        PowerMockito.whenNew(TableConfigurationUtil.class).withAnyArguments().thenReturn(tcu);
        PowerMockito.doNothing().when(tcu).setTableItersPrioritiesAndOpts();
        PowerMockito.when(tcu.getTableCombiners(Mockito.eq("table1"))).thenReturn(table1CombinerMap);

        AggregatingReducerTest.TestAggregatingReducer aggregatingReducer = new AggregatingReducerTest.TestAggregatingReducer();
        aggregatingReducer.configureCombiners(conf);
    }

    private void assertCombineList(String[] expected, AggregatingReducerTest.TestAggregatingReducer aggregatingReducer, Text table, Key key) {
        List<Combiner> combiners = aggregatingReducer.getAggregators(table, key);
        List<String> actual = new ArrayList();

        combiners.forEach(combiner -> actual.add(combiner.toString()));
        Assert.assertArrayEquals(expected, actual.toArray());
    }

    private class TestAggregatingReducer extends AggregatingReducer<String,String,String,String> {}

    public static class testCombiner3 extends AggregatingReducerTest.testCombiner {
        public testCombiner3() {}
    }

    public static class testCombiner2 extends AggregatingReducerTest.testCombiner {
        public testCombiner2() {}
    }

    public static class testCombiner1 extends AggregatingReducerTest.testCombiner {
        public testCombiner1() {}
    }

    public static class testCombiner extends Combiner {
        public testCombiner() {}

        public Value reduce(Key key, Iterator<Value> iter) {
            return null;
        }

        public String toString() {
            return this.getClass().getSimpleName();
        }
    }
}
