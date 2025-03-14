package datawave.ingest.table.filter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import datawave.data.ColumnFamilyConstants;
import datawave.query.iterator.SortedListKeyValueIterator;

public class WhindexCreationDateFilterTest {

    public static class MockIteratorEnvironment implements IteratorEnvironment {
        AccumuloConfiguration conf;
        private final boolean major;
        private final boolean isUser;

        public MockIteratorEnvironment(boolean major) {
            this(major, false);
        }

        public MockIteratorEnvironment(boolean major, boolean isUser) {
            this.conf = DefaultConfiguration.getInstance();
            this.major = major;
            this.isUser = isUser;
        }

        @Override
        public AccumuloConfiguration getConfig() {
            return conf;
        }

        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
            if (major) {
                return IteratorUtil.IteratorScope.majc;
            } else
                return IteratorUtil.IteratorScope.scan;
        }

        @Override
        public boolean isFullMajorCompaction() {
            return major;
        }

        @Override
        public boolean isUserCompaction() {
            return isUser;
        }

        @Override
        public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Authorizations getAuthorizations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            throw new SampleNotPresentException();
        }

        @Override
        public boolean isSamplingEnabled() {
            return false;
        }

        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return null;
        }

        @Override
        public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String arg0) {
            return null;
        }
    }

    private static final Value LONG_VALUE = new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L));
    private static final Value NULL_VALUE = new Value(new byte[0]);
    private static final long TIMESTAMP = 1743520565142L;

    /**
     * Tests three cases:
     * <ol>
     * <li>No whindex entries (APPLE)</li>
     * <li>Single whindex entry (BANANA)</li>
     * <li>Multiple whindex entries (KIWI)</li>
     * </ol>
     */
    @Test
    public void testWhindexFilter() throws IOException {

        TreeMap<Key,Value> keys = new TreeMap<>();

        // No whindex entry
        keys.put(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_F, new Text("csv" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_I, new Text("csv" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_F, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_I, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);

        // single whindex entry
        keys.put(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_F, new Text("csv" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_I, new Text("csv" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55550101"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_F, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_I, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);

        // multiple whindex entries
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_F, new Text("csv" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_I, new Text("csv" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55550101"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55550102"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55550201"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55560101"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_F, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_I, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), LONG_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("wiki" + "\0" + "55550101"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("wiki" + "\0" + "55550102"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("wiki" + "\0" + "55550201"), TIMESTAMP), NULL_VALUE);
        keys.put(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("wiki" + "\0" + "55560101"), TIMESTAMP), NULL_VALUE);

        MockIteratorEnvironment iterEnv = new MockIteratorEnvironment(true);
        SortedKeyValueIterator<Key,Value> source = new SortedListKeyValueIterator(keys);
        SortedKeyValueIterator<Key,Value> filter = new WhindexCreationDateFilter();
        filter.init(source, new HashMap<>(), iterEnv);

        List<Map.Entry<Key,Value>> expected = new ArrayList<>();
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_F, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_F, new Text("wiki" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_I, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("APPLE"), ColumnFamilyConstants.COLF_I, new Text("wiki" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_F, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_F, new Text("wiki\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_I, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_I, new Text("wiki\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("BANANA"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        NULL_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_F, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_F, new Text("wiki\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_I, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_I, new Text("wiki\0" + "55550101"), TIMESTAMP),
                        LONG_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("csv" + "\0" + "55550101"), TIMESTAMP),
                        NULL_VALUE));
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text("KIWI"), ColumnFamilyConstants.COLF_WCD, new Text("wiki\0" + "55550101"), TIMESTAMP),
                        NULL_VALUE));

        List<Map.Entry<Key,Value>> actual = new ArrayList<>();
        filter.seek(new Range(), List.of(), false);
        while (filter.hasTop()) {
            actual.add(new AbstractMap.SimpleEntry<>(filter.getTopKey(), filter.getTopValue()));
            filter.next();
        }

        assertEquals(expected, actual);
    }
}
