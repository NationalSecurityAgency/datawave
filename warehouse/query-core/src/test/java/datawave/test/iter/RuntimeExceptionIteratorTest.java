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
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class RuntimeExceptionIteratorTest {

    private final Value EMPTY_VALUE = new Value();

    @Test
    public void testNoExceptionClassOption() {
        Map<String,String> options = Collections.emptyMap();
        initializeIteratorWithOptions(options);
    }

    @Test
    public void testNoExceptionMessageOption() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, NullPointerException.class.getName());
        initializeIteratorWithOptions(options);
    }

    @Test
    public void testNoFireOption() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, NullPointerException.class.getName());
        options.put(RuntimeExceptionIterator.EXCEPTION_MESSAGE, "NPE for test");
        initializeIteratorWithOptions(options);
    }

    private void initializeIteratorWithOptions(Map<String,String> options) {
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(Collections.emptySortedMap());
        RuntimeExceptionIterator iter = new RuntimeExceptionIterator();
        assertThrows(IllegalStateException.class, () -> iter.init(source, options, new TestIteratorEnv()));
    }

    @Test
    public void testExceptionOnSeek() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, RuntimeException.class.getName());
        options.put(RuntimeExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(RuntimeExceptionIterator.FIRE_ON_SEEK, "true");
        assertThrows(RuntimeException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnNext() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, RuntimeException.class.getName());
        options.put(RuntimeExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(RuntimeExceptionIterator.FIRE_ON_NEXT, "true");
        assertThrows(RuntimeException.class, () -> driveIterator(options));
    }

    @Test
    public void testExceptionOnRandomOperation() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, RuntimeException.class.getName());
        options.put(RuntimeExceptionIterator.EXCEPTION_MESSAGE, "RuntimeException msg");
        options.put(RuntimeExceptionIterator.FIRE_RANDOMLY, "true");
        assertThrows(RuntimeException.class, () -> driveIterator(options));
    }

    @Test
    public void testNullPointerException() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, NullPointerException.class.getName());
        options.put(RuntimeExceptionIterator.EXCEPTION_MESSAGE, "NPE for test");
        options.put(RuntimeExceptionIterator.FIRE_ON_NEXT, "true");
        assertThrows(NullPointerException.class, () -> driveIterator(options));
    }

    @Test
    public void testIterationInterruptedException() {
        Map<String,String> options = new HashMap<>();
        options.put(RuntimeExceptionIterator.EXCEPTION_CLASS, IterationInterruptedException.class.getName());
        options.put(RuntimeExceptionIterator.EXCEPTION_MESSAGE, "NPE for test");
        options.put(RuntimeExceptionIterator.FIRE_ON_NEXT, "true");
        assertThrows(IterationInterruptedException.class, () -> driveIterator(options));
    }

    private void driveIterator(Map<String,String> options) throws Exception {
        SortedKeyValueIterator<Key,Value> source = getData();
        RuntimeExceptionIterator iter = new RuntimeExceptionIterator();
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
