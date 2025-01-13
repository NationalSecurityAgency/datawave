package datawave.query.util.sortedmap;

import static org.junit.Assert.assertEquals;

import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class BufferedFileBackedKeyValueSortedMapTest extends BufferedFileBackedRewritableSortedMapTest<Key,Value> {

    private Comparator<Key> keyComparator = new Comparator<>() {
        @Override
        public int compare(Key o1, Key o2) {
            return o1.compareTo(o2);
        }
    };

    private FileSortedMap.RewriteStrategy<Key,Value> keyValueComparator = new FileSortedMap.RewriteStrategy<>() {
        @Override
        public boolean rewrite(Key key, Value original, Value update) {
            return original.compareTo(update) < 0;
        }
    };

    @Override
    public FileSortedMap.RewriteStrategy<Key,Value> getRewriteStrategy() {
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
    public void testEquality(Map.Entry<Key,Value> expected, Map.Entry<Key,Value> value) {
        assertEquals(expected.getKey(), value.getKey());
        assertEquals(expected.getValue(), value.getValue());
    }

    @Override
    public Comparator<Key> getComparator() {
        return keyComparator;
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<Key,Value> getFactory() {
        return new FileKeyValueSortedMap.Factory();
    }

}
