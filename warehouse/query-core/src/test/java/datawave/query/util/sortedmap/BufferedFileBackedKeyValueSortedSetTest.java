package datawave.query.util.sortedmap;

import datawave.query.util.sortedset.FileSortedSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Comparator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BufferedFileBackedKeyValueSortedSetTest extends BufferedFileBackedRewritableSortedSetTest<Key,Value> {

    private Comparator<Map.Entry<Key,Value>> keyComparator = new Comparator<>() {
        @Override
        public int compare(Map.Entry<Key,Value> o1, Map.Entry<Key,Value> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    private RewritableSortedSetImpl.RewriteStrategy<Map.Entry<Key,Value>> keyValueComparator = new RewritableSortedSetImpl.RewriteStrategy<>() {
        @Override
        public boolean rewrite(Map.Entry<Key,Value> original, Map.Entry<Key,Value> update) {
            int comparison = original.getKey().compareTo(update.getKey());
            if (comparison == 0) {
                comparison = original.getValue().compareTo(update.getValue());
            }
            return comparison < 0;
        }
    };

    @Override
    public RewritableSortedSet.RewriteStrategy<Map.Entry<Key,Value>> getRewriteStrategy() {
        return keyValueComparator;
    }

    @Override
    public Key createKey(byte[] values) {
        return new Key(values);
    }

    @Override
    public Value createValue(byte[] values) {
        return new Value(values);
    }

    @Override
    public void testFullEquality(Map.Entry<Key,Value> expected, Map.Entry<Key,Value> value) {
        assertEquals(expected.getKey(), value.getKey());
        assertEquals(expected.getValue(), value.getValue());
    }

    @Override
    public Comparator<Map.Entry<Key,Value>> getComparator() {
        return keyComparator;
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<Map.Entry<Key,Value>> getFactory() {
        return new FileKeyValueSortedMap.Factory();
    }

}
