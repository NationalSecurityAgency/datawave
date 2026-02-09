package datawave.test.iter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class DelayIteratorTest {

    private final Value EMPTY_VALUE = new Value();

    @Test
    public void testNoOptions() {
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(Collections.emptySortedMap());
        DelayIterator iter = new DelayIterator();
        assertThrows(IllegalStateException.class, () -> iter.init(source, Collections.emptyMap(), new TestIteratorEnv()));
    }

    @Test
    public void testSlowIteration() throws IOException {
        int delay = 5;
        int numKeys = 10;

        Set<Key> expected = new HashSet<>();
        for (int i = 0; i < numKeys; i++) {
            expected.add(new Key("row-" + i));
        }

        TreeMap<Key,Value> data = new TreeMap<>();
        for (Key key : expected) {
            data.put(key, EMPTY_VALUE);
        }

        Map<String,String> options = new HashMap<>();
        options.put(DelayIterator.DELAY_MILLIS, String.valueOf(delay));

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
        DelayIterator iter = new DelayIterator();

        iter.init(source, options, new TestIteratorEnv());

        Set<Key> results = new HashSet<>();
        long start = System.currentTimeMillis();
        iter.seek(new Range(), Collections.emptySet(), false);
        while (iter.hasTop()) {
            results.add(iter.getTopKey());
            iter.next();
        }
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);

        // time is 5ms per key, plus initial seek for a total of 55ms
        int expectedTime = (numKeys * delay) + delay;
        assertTrue(elapsed >= expectedTime);
    }

}
