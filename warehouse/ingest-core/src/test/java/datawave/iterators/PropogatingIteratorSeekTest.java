package datawave.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.table.aggregator.PropogatingCombiner;

public class PropogatingIteratorSeekTest {
    public static final String FIELD_NO_AGGREGATION = "a";
    public static final String FIELD_TO_AGGREGATE = "b";
    public static final String FIELD2_NO_AGGREGATION = "c";
    public static final String COL_QUAL = "colQual1";
    private PropogatingIterator iterator;
    private TreeMap<Key,Value> expected;

    @Before
    public void before() throws IOException {
        SortedMap<Key,Value> map = new TreeMap<>();
        map.put(new Key("row", FIELD_NO_AGGREGATION, COL_QUAL, "", 3), new Value(new Text("magnolia")));
        map.put(new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 4), new Value(new Text("1")));
        map.put(new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 3), new Value(new Text("2")));
        map.put(new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 2), new Value(new Text("1")));
        map.put(new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 1), new Value(new Text("6")));
        map.put(new Key("row", FIELD2_NO_AGGREGATION, COL_QUAL, "", 3), new Value(new Text("milkweed")));
        map.put(new Key("row", FIELD2_NO_AGGREGATION, COL_QUAL, "", 2), new Value(new Text("bayberry")));
        map.put(new Key("row", FIELD2_NO_AGGREGATION, COL_QUAL, "", 1), new Value(new Text("dogwood")));
        SortedMapIterator source = new SortedMapIterator(map);

        expected = new TreeMap<>();
        expected.put(new Key("row", FIELD_NO_AGGREGATION, COL_QUAL, "", 3), new Value(new Text("magnolia")));
        // the only key that's values should be aggregated:
        expected.put(new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 4), new Value(new Text("10")));
        expected.put(new Key("row", FIELD2_NO_AGGREGATION, COL_QUAL, "", 3), new Value(new Text("milkweed")));
        expected.put(new Key("row", FIELD2_NO_AGGREGATION, COL_QUAL, "", 2), new Value(new Text("bayberry")));
        expected.put(new Key("row", FIELD2_NO_AGGREGATION, COL_QUAL, "", 1), new Value(new Text("dogwood")));

        Map<String,String> options = new HashMap<>();
        options.put(FIELD_TO_AGGREGATE, TestCombiner.class.getName());

        TestIteratorEnvironment env = new TestIteratorEnvironment();

        iterator = new PropogatingIterator();
        iterator.init(source, options, env);
    }

    @Test
    public void testReseeksOverFullRange() throws IOException {
        // reseek after each key
        verifyAllDataFound(expected, new TestData(iterator, new Range(new Key("row"), null), true).data);
    }

    @Test
    public void testNextCallsOverFullRange() throws IOException {
        // seek once, then call next through all the data
        verifyAllDataFound(expected, new TestData(iterator, new Range(new Key("row"), null), false).data);
    }

    @Test
    public void startMidwayThroughTimestamps() throws IOException {
        // start partway through data for field to aggregate
        Key startKey = new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 3);
        Key endKey = new Key("row\\x00");
        Range range = new Range(startKey, true, endKey, false);

        // don't expect the key that precedes the start key
        Assert.assertNotNull(expected.remove(new Key("row", FIELD_NO_AGGREGATION, COL_QUAL, "", 3)));

        // the existing behavior - when starting partway through timestamps, skip key
        Assert.assertNotNull(expected.remove(new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 4)));

        // try both with the reseek and the next approaches
        verifyAllDataFound(expected, new TestData(iterator, range, false).data);
        verifyAllDataFound(expected, new TestData(iterator, range, true).data);
    }

    @Test
    public void startJustBeforeFirstTimestamp() throws IOException {
        // start before data for field to aggregate
        Key startKey = new Key("row", FIELD_TO_AGGREGATE, COL_QUAL, "", 5);
        Key endKey = new Key("row\\x00");
        Range range = new Range(startKey, true, endKey, false);

        // don't expect the key that precedes the start key
        Assert.assertNotNull(expected.remove(new Key("row", FIELD_NO_AGGREGATION, COL_QUAL, "", 3)));

        // try both with the reseek and the next approaches
        verifyAllDataFound(expected, new TestData(iterator, range, false).data);
        verifyAllDataFound(expected, new TestData(iterator, range, true).data);
    }

    private void verifyAllDataFound(TreeMap<Key,Value> expected, SortedMap<Key,Value> actual) {
        Assert.assertEquals(expected, actual);
    }

    public static class TestCombiner extends PropogatingCombiner {
        public TestCombiner() {}

        @Override
        public Value reduce(Key key, Iterator<Value> valueIterator) {
            int total = 0;
            while (valueIterator.hasNext()) {
                total += getCount(valueIterator.next());
            }
            return new Value(new Text(Integer.toString(total)));
        }

        private static int getCount(Value value) {
            return Integer.parseInt(new Text(value.get()).toString(), 10);
        }
    }

    public static class TestIteratorEnvironment implements IteratorEnvironment {
        public boolean isSamplingEnabled() {
            return false;
        }

        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return null;
        }

        @Override
        public boolean isUserCompaction() {
            return false;
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
            return null;
        }

        @Override
        public PluginEnvironment getPluginEnv() {
            return null;
        }

        @Override
        public TableId getTableId() {
            return null;
        }

        @Override
        public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String s) throws IOException {
            return null;
        }

        public IteratorUtil.IteratorScope getIteratorScope() {
            return IteratorUtil.IteratorScope.minc;
        }

        @Override
        public boolean isFullMajorCompaction() {
            return false;
        }

        @Override
        public void registerSideChannel(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator) {

        }

        @Override
        public Authorizations getAuthorizations() {
            return null;
        }

        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            return null;
        }
    }

    public static class TestData {
        final SortedMap<Key,Value> data = new TreeMap<>();

        // copied from Fluo
        public TestData(SortedKeyValueIterator<Key,Value> iter, Range range, boolean reseek) {
            try {
                iter.seek(range, new HashSet<>(), false);

                while (iter.hasTop()) {
                    data.put(iter.getTopKey(), iter.getTopValue());
                    if (reseek) {
                        iter.seek(new Range(iter.getTopKey(), false, range.getEndKey(), range.isEndKeyInclusive()), new HashSet<>(), false);
                    } else {
                        iter.next();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
