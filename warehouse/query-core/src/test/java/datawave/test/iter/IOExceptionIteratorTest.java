package datawave.test.iter;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

import datawave.core.iterators.IteratorTimeoutException;

public class IOExceptionIteratorTest {
    private final Value EMPTY_VALUE = new Value();

    @Test
    public void testNoExceptionClassOption() {
        Map<String,String> options = Collections.emptyMap();
        initializeIteratorWithOptions(options);
    }

    @Test
    public void testNoExceptionMessageOption() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        initializeIteratorWithOptions(options);
    }

    @Test
    public void testNoFireOption() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "NPE for test");
        initializeIteratorWithOptions(options);
    }

    private void initializeIteratorWithOptions(Map<String,String> options) {
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(Collections.emptySortedMap());
        IOExceptionIterator iter = new IOExceptionIterator();
        assertThrows(IllegalStateException.class, () -> iter.init(source, options, new TestIteratorEnv()));
    }

    @Test
    public void testExceptionOnSeek() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(IOExceptionIterator.FIRE_ON_SEEK, "true");
        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnNext() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(IOExceptionIterator.FIRE_ON_NEXT, "true");
        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnRandomOperation() {
        Map<String,String> options = new HashMap<>();
        options.put(IOExceptionIterator.EXCEPTION_CLASS, IteratorTimeoutException.class.getName());
        options.put(IOExceptionIterator.EXCEPTION_MESSAGE, "IOException msg");
        options.put(IOExceptionIterator.FIRE_RANDOMLY, "true");
        assertThrows(IteratorTimeoutException.class, () -> driveIterator(options));
    }

    private void driveIterator(Map<String,String> options) throws Exception {
        SortedKeyValueIterator<Key,Value> source = getData();
        IOExceptionIterator iter = new IOExceptionIterator();
        iter.init(source, options, new TestIteratorEnv());
        iter.seek(new Range(), Collections.emptySet(), false);
        while (iter.hasTop()) {
            iter.next();
        }
    }

    private SortedKeyValueIterator<Key,Value> getData() {
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < 50; i++) {
            data.put(new Key("row-" + i), EMPTY_VALUE);
        }
        return new SortedMapIterator(data);
    }
}
